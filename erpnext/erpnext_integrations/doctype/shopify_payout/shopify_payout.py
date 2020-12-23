# -*- coding: utf-8 -*-
# Copyright (c) 2020, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

from collections import defaultdict

from shopify import Payouts, Order, Transaction

import frappe
from erpnext.accounts.doctype.sales_invoice.sales_invoice import make_sales_return
from erpnext.erpnext_integrations.connectors.shopify_connection import (
	create_shopify_delivery, create_shopify_invoice, create_shopify_order,
	get_tax_account_head)
from erpnext.erpnext_integrations.doctype.shopify_log.shopify_log import make_shopify_log
from erpnext.erpnext_integrations.doctype.shopify_settings.sync_payout import (
	create_or_update_shopify_payout, get_shopify_document)
from frappe.model.document import Document
from frappe.utils import cint


class ShopifyPayout(Document):
	settings = frappe.get_single("Shopify Settings")

	def before_submit(self):
		"""
		Before submitting a Payout, check the following:

			- Update the Payout with the latest info from Shopify (WIP)
			- Create missing order documents for any Shopify Order
		"""

		# TODO: self.update_shopify_payout()
		self.create_missing_orders()

	def on_submit(self):
		"""
		On submit of a Payout, do the following:

			- If a Shopify Order is cancelled, update all linked documents in ERPNext
			- If a Shopify Order has been fully returned, make a sales return in ERPNext
			- Create a Journal Entry to balance all existing transactions
				with additional fees and charges from Shopify, if any
		"""

		self.update_cancelled_shopify_orders()
		self.create_sales_returns()
		self.create_payout_journal_entry()

	# def update_shopify_payout(self):
	# 	session = self.settings.get_shopify_session()
	# 	Payouts.activate_session(session)
	# 	payout = Payouts.find(cint(self.payout_id))
	# 	Payouts.clear_session()
	# 	create_or_update_shopify_payout(payout, payout_doc=self)
	# 	self.load_from_db()

	def create_missing_orders(self):
		for transaction in self.transactions:
			shopify_order_id = transaction.source_order_id

			if not shopify_order_id:
				continue

			session = self.settings.get_shopify_session()
			Order.activate_session(session)
			order = Order.find(cint(shopify_order_id))
			Order.clear_session()

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
					delivery_note = delivery_notes[0] if delivery_notes and len(delivery_notes) > 0 else None

			# update the transaction with the linked documents
			transaction.update({
				"sales_order": sales_order,
				"sales_invoice": sales_invoice,
				"delivery_note": delivery_note
			})

	def update_cancelled_shopify_orders(self):
		doctypes = ["Delivery Note", "Sales Invoice", "Sales Order"]
		for transaction in self.transactions:
			if not transaction.source_order_id:
				continue

			session = self.settings.get_shopify_session()
			Order.activate_session(session)
			shopify_order = Order.find(cint(transaction.source_order_id))
			Order.clear_session()

			if not shopify_order:
				continue

			if not shopify_order.cancelled_at:
				continue

			for doctype in doctypes:
				doctype_field = frappe.scrub(doctype)
				docname = transaction.get(doctype_field)

				if not docname:
					continue

				doc = frappe.get_doc(doctype, docname)

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

	def create_sales_returns(self):
		invoices = {transaction.sales_invoice: transaction.source_order_id for transaction in self.transactions
			if transaction.sales_invoice and transaction.source_order_id}

		for sales_invoice_id, shopify_order_id in invoices.items():
			session = self.settings.get_shopify_session()
			Order.activate_session(session)
			shopify_order = Order.find(cint(shopify_order_id))
			Order.clear_session()

			if not shopify_order:
				continue

			# TODO: handle partial refunds
			is_order_refunded = shopify_order.financial_status == "refunded"
			is_invoice_returned = frappe.db.get_value("Sales Invoice", sales_invoice_id, "status") in ["Return",
				"Credit Note Issued"]

			if is_order_refunded and not is_invoice_returned:
				# TODO: use correct posting date for returns
				return_invoice = make_sales_return(sales_invoice_id)
				return_invoice.save()
				return_invoice.submit()

	def create_payout_journal_entry(self):
		entries = []

		# make payout cash entry
		for transaction in self.transactions:
			if transaction.transaction_type.lower() == "payout":
				if transaction.total_amount:
					entries.append(get_amount_entry(transaction))

				if transaction.fee:
					entries.append(get_fee_entry(self, transaction))

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

				if transaction.fee:
					entries.append(get_fee_entry(self, transaction, references))

		if entries:
			journal_entry = frappe.new_doc("Journal Entry")
			journal_entry.posting_date = frappe.utils.today()
			journal_entry.set("accounts", entries)
			journal_entry.save()
			# journal_entry.submit()


def get_amount_entry(transaction, references=None):
	if not references:
		references = {}

	account = None
	if transaction.transaction_type:
		account = get_tax_account_head({"title": transaction.transaction_type})

	if not account:
		if references.get("reference_name"):
			account = frappe.db.get_value(references.get(
				"reference_type"), references.get("reference_name"), "debit_to")
		else:
			# TODO: change to a different default
			account = get_tax_account_head({"title": "Payout"})

	return get_accounting_entry(
		account=account,
		amount=transaction.total_amount,
		**references
	)


def get_fee_entry(payout, transaction, references=None):
	if not references:
		references = {}

	session = payout.settings.get_shopify_session()
	Transaction.activate_session(session)
	order_transaction = Transaction.find(
		transaction.source_order_transaction_id,
		order_id=transaction.source_order_id
	)
	Transaction.clear_session()

	account = None
	if hasattr(order_transaction.receipt, "balance_transaction"):
		fee_details = order_transaction.receipt.balance_transaction.fee_details
		transaction_type = fee_details[0].description
		account = get_tax_account_head({"title": transaction_type})

	if not account:
		if references.get("reference_name"):
			account = frappe.db.get_value(references.get(
				"reference_type"), references.get("reference_name"), "debit_to")
		else:
			# TODO: change to a different default
			account = get_tax_account_head({"title": "Payout"})

	return get_accounting_entry(
		account=account,
		amount=-transaction.fee,
		**references
	)


def get_accounting_entry(
	account,
	amount,
	reference_type=None,
	reference_name=None,
	party_type=None,
	party_name=None,
	remark=None
):
	accounting_entry = frappe._dict({
		"account": account,
		"reference_type": reference_type,
		"reference_name": reference_name,
		"party_type": party_type,
		"party": party_name,
		"user_remark": remark
	})

	accounting_entry[get_debit_or_credit(amount, account)] = abs(amount)
	return accounting_entry


def get_debit_or_credit(amount, account):
	root_type, account_type = frappe.get_cached_value(
		"Account", account, ["root_type", "account_type"]
	)

	debit_field = "debit_in_account_currency"
	credit_field = "credit_in_account_currency"

	if root_type == "Asset":
		if account_type in ("Receivable", "Payable"):
			return debit_field if amount < 0 else credit_field
		return debit_field if amount > 0 else credit_field
	elif root_type == "Expense":
		return debit_field if amount < 0 else credit_field
	elif root_type == "Income":
		return debit_field if amount > 0 else credit_field
	elif root_type in ("Equity", "Liability"):
		if account_type in ("Receivable", "Payable"):
			return debit_field if amount > 0 else credit_field
		else:
			return debit_field if amount < 0 else credit_field
