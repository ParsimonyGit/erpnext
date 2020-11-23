# -*- coding: utf-8 -*-
# Copyright (c) 2020, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

from collections import defaultdict

from shopify import Order

import frappe
from erpnext.accounts.doctype.sales_invoice.sales_invoice import make_sales_return
from erpnext.erpnext_integrations.connectors.shopify_connection import (
	create_shopify_delivery, create_shopify_invoice, create_shopify_order)
from erpnext.erpnext_integrations.doctype.shopify_settings.sync_payout import (
	Payouts, create_or_update_shopify_payout, get_shopify_document)
from frappe.model.document import Document
from frappe.utils import cint
from erpnext.erpnext_integrations.doctype.shopify_log.shopify_log import make_shopify_log


class ShopifyPayout(Document):
	settings = frappe.get_single("Shopify Settings")

	def before_submit(self):
		"""
		Before submitting a Payout, check the following:

			- Update the Payout with the latest info from Shopify (WIP)
			- Create missing order documents for any Shopify Order
		"""

		# self.update_shopify_payout()
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
	# 	with self.settings.get_shopify_session(temp=True):
	# 		payout = Payouts.find(cint(self.payout_id))
	# 		create_or_update_shopify_payout(payout, payout_doc=self)
	# 		self.load_from_db()

	def create_missing_orders(self):
		for transaction in self.transactions:
			shopify_order_id = transaction.source_order_id

			# create an order, invoice and delivery, if missing
			# if shopify_order_id and not get_shopify_document("Sales Order", shopify_order_id):
			with self.settings.get_shopify_session(temp=True):
				order = Order.find(cint(shopify_order_id))
				if not order:
					continue

				# TODO: use correct posting date for returns
				so = create_shopify_order(order.to_dict())
				if so:
					create_shopify_invoice(order.to_dict(), so)
					create_shopify_delivery(order.to_dict(), so)

			transaction.update({
				"sales_order": get_shopify_document("Sales Order", shopify_order_id),
				"sales_invoice": get_shopify_document("Sales Invoice", shopify_order_id),
				"delivery_note": get_shopify_document("Delivery Note", shopify_order_id)
			})

	def update_cancelled_shopify_orders(self):
		doctypes = ["Delivery Note", "Sales Invoice", "Sales Order"]
		for transaction in self.transactions:
			if not transaction.source_order_id:
				continue

			with self.settings.get_shopify_session(temp=True):
				shopify_order = Order.find(cint(transaction.source_order_id))
				if not shopify_order:
					continue

			if not (shopify_order and shopify_order.cancelled_at):
				continue

			for doctype in doctypes:
				doctype_field = frappe.scrub(doctype)
				docname = transaction.get(doctype_field)
				if docname:
					doc = frappe.get_doc(doctype, docname)

					# do not cancel refunded orders
					if doctype == "Sales Invoice" and doc.status in ["Return", "Credit Note Issued"]:
						continue

					# allow cancelling invoices and maintaining links with payout
					doc.ignore_linked_doctypes = ["Shopify Payout"]

					try:
						doc.cancel()
					except Exception as e:
						make_shopify_log(status="Error", exception=e)

					transaction.set(doctype_field, None)

	def create_sales_returns(self):
		invoices = {transaction.sales_invoice: transaction.source_order_id for transaction in self.transactions
			if transaction.sales_invoice and transaction.source_order_id}

		for sales_invoice_id, shopify_order_id in invoices.items():
			with self.settings.get_shopify_session(temp=True):
				shopify_order = Order.find(cint(shopify_order_id))
				if not shopify_order:
					continue

			is_order_refunded = shopify_order.financial_status == "refunded"
			is_invoice_returned = frappe.db.get_value("Sales Invoice", sales_invoice_id, "status") in ["Return",
				"Credit Note Issued"]

			if is_order_refunded and not is_invoice_returned:
				# TODO: use correct posting date for returns
				return_invoice = make_sales_return(sales_invoice_id)
				return_invoice.save()
				return_invoice.submit()

	def create_payout_journal_entry(self):
		payouts_by_invoice = defaultdict(list)
		for transaction in self.transactions:
			if transaction.sales_invoice:
				payouts_by_invoice[transaction.sales_invoice].append(transaction)

		for invoice_id, order_transactions in payouts_by_invoice.items():
			for transaction in order_transactions:
				transaction_type = transaction.transaction_type.lower()

				# check for charges / payouts
				if transaction_type == "charge":
					# TODO
					pass
				# check for adjustments
				elif transaction_type == "adjustment":
					# TODO
					pass
				# check for refunds
				elif transaction_type == "refund":
					# TODO
					pass
