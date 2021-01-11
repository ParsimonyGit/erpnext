# -*- coding: utf-8 -*-
# Copyright (c) 2020, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

from collections import defaultdict

from shopify import Order

import frappe
from erpnext.controllers.accounts_controller import get_accounting_entry
from erpnext.erpnext_integrations.connectors.shopify_connection import (
	create_sales_return, create_shopify_delivery, create_shopify_invoice,
	create_shopify_order, get_shopify_document, get_tax_account_head)
from erpnext.erpnext_integrations.doctype.shopify_log.shopify_log import make_shopify_log
from frappe.model.document import Document
from frappe.utils import cint, flt


class ShopifyPayout(Document):
	settings = frappe.get_single("Shopify Settings")

	def before_submit(self):
		"""
		Before submitting a Payout, check the following:

			- Create missing order documents for any Shopify Order
		"""

		self.create_missing_orders()

	def on_submit(self):
		"""
		On submit of a Payout, do the following:

			- If a Shopify Order is cancelled, update all linked documents in ERPNext
			- Find a draft Sales Invoice against the Shopify order, update it with
				new payment fees and charges, and submit the invoice
			- If a Shopify Order has been fully returned, make a sales return in ERPNext
			- Create a Journal Entry to balance all existing transactions
				with additional fees and charges from Shopify, if any
		"""

		self.update_cancelled_shopify_orders()
		self.update_shopify_payment_fees()
		self.create_sales_returns()
		self.create_payout_journal_entry()

	def create_missing_orders(self):
		session = self.settings.get_shopify_session()
		Order.activate_session(session)

		for transaction in self.transactions:
			shopify_order_id = transaction.source_order_id
			if not shopify_order_id:
				continue

			order = Order.find(cint(shopify_order_id))
			if not order:
				continue

			sales_order = get_shopify_document("Sales Order", shopify_order_id)
			sales_invoice = get_shopify_document("Sales Invoice", shopify_order_id)
			delivery_note = get_shopify_document("Delivery Note", shopify_order_id)

			# create an order, invoice and delivery, if missing
			if not sales_order:
				sales_order = create_shopify_order(order.to_dict())

			if sales_order:
				if not sales_invoice:
					sales_invoice = create_shopify_invoice(order.to_dict(), sales_order)
				if not delivery_note:
					delivery_notes = create_shopify_delivery(order.to_dict(), sales_order)
					delivery_note = delivery_notes[0] if delivery_notes and \
						len(delivery_notes) > 0 else frappe._dict()

			# update the transaction with the linked documents
			transaction.update({
				"sales_order": sales_order.name,
				"sales_invoice": sales_invoice.name,
				"delivery_note": delivery_note.name
			})

		Order.clear_session()

	def update_cancelled_shopify_orders(self):
		doctypes = ["Delivery Note", "Sales Invoice", "Sales Order"]

		session = self.settings.get_shopify_session()
		Order.activate_session(session)

		for transaction in self.transactions:
			if not transaction.source_order_id:
				continue

			shopify_order = Order.find(cint(transaction.source_order_id))
			if not shopify_order or not shopify_order.cancelled_at:
				continue

			for doctype in doctypes:
				doctype_field = frappe.scrub(doctype)
				docname = transaction.get(doctype_field)

				if not docname:
					continue

				doc = frappe.get_doc(doctype, docname)

				# do not try and cancel draft or cancelled documents
				if doc.docstatus != 1:
					continue

				# do not cancel refunded orders
				if doctype == "Sales Invoice" and doc.status in ["Return", "Credit Note Issued"]:
					continue

				# allow cancelling invoices and maintaining links with payout
				doc.ignore_linked_doctypes = ["Shopify Payout"]

				# catch any other errors and log it
				try:
					doc.cancel()
				except Exception as e:
					make_shopify_log(status="Error", exception=e)

				transaction.db_set(doctype_field, None)

		Order.clear_session()

	def update_shopify_payment_fees(self):
		payouts_by_invoice = defaultdict(list)
		for transaction in self.transactions:
			if transaction.sales_invoice:
				payouts_by_invoice[transaction.sales_invoice].append(transaction)

		for invoice_id, order_transactions in payouts_by_invoice.items():
			invoice = frappe.get_doc("Sales Invoice", invoice_id)
			if invoice.docstatus != 0:
				continue

			for transaction in order_transactions:
				if not transaction.fee:
					continue

				invoice.append("taxes", {
					"charge_type": "Actual",
					"account_head": get_tax_account_head({"title": transaction.transaction_type}),
					"description": transaction.transaction_type,
					"tax_amount": flt(transaction.fee)
				})

			invoice.save()
			invoice.submit()

	def create_sales_returns(self):
		transactions = [transaction for transaction in self.transactions
			if transaction.sales_invoice and transaction.source_order_id]

		if not transactions:
			return

		for transaction in transactions:
			financial_status = frappe.scrub(transaction.source_order_financial_status)

			if financial_status not in ["refunded", "partially_refunded"]:
				continue

			is_invoice_returned = frappe.db.get_value("Sales Invoice", transaction.sales_invoice, "status") in \
				["Return", "Credit Note Issued"]

			if not is_invoice_returned:
				si_doc = frappe.get_doc("Sales Invoice", transaction.sales_invoice)
				create_sales_return(transaction.source_order_id, financial_status, si_doc)

	def create_payout_journal_entry(self):
		entries = []

		# make payout cash entry
		for transaction in self.transactions:
			if transaction.transaction_type.lower() == "payout":
				if transaction.total_amount:
					entries.append(get_amount_entry(transaction))

		# get the list of transactions that need to be balanced
		payouts_by_invoice = defaultdict(list)
		for transaction in self.transactions:
			if transaction.sales_invoice:
				payouts_by_invoice[transaction.sales_invoice].append(transaction)

		# generate journal entries for each missing transaction
		for invoice_id, order_transactions in payouts_by_invoice.items():
			reference_type = "Sales Invoice"
			reference_name = invoice_id
			party_type = "Customer"
			party_name = frappe.get_cached_value("Sales Invoice", invoice_id, "customer")

			for transaction in order_transactions:
				references = dict(
					reference_type=reference_type,
					reference_name=reference_name,
					party_type=party_type,
					party_name=party_name
				)

				if transaction.total_amount:
					entries.append(get_amount_entry(transaction, references))

		if entries:
			journal_entry = frappe.new_doc("Journal Entry")
			journal_entry.posting_date = frappe.utils.today()
			journal_entry.set("accounts", entries)
			journal_entry.save()
			journal_entry.submit()


def get_amount_entry(transaction, references=None):
	"""
	Get the Journal Entry accounting entry for a Shopify transaction.

	Args:
		transaction (ShopifyPayoutTransaction): The Shopify Payout transaction data.
		references (dict, optional): The document references for the Shopify transaction.
			Defaults to None.

	Raises:
		frappe.ValidationError: If a transaction's charge or tax type is not
			mapped to an account head in Shopify Settings.

	Returns:
		frappe._dict: The Journal Entry accounting entry data.
	"""

	if not references:
		references = {}

	account = None
	amount = flt(transaction.net_amount)

	if transaction.transaction_type:
		try:
			account = get_tax_account_head({"title": transaction.transaction_type})
		except frappe.ValidationError as e:
			if references.get("reference_name"):
				account = frappe.db.get_value(references.get(
					"reference_type"), references.get("reference_name"), "debit_to")

			if not account:
				raise e

	return get_accounting_entry(
		account=account,
		amount=amount,
		**references
	)
