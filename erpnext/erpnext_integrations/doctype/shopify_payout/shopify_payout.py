# -*- coding: utf-8 -*-
# Copyright (c) 2020, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

from collections import defaultdict

from shopify import Order

import frappe
from erpnext.accounts.doctype.sales_invoice.sales_invoice import make_sales_return
from erpnext.erpnext_integrations.connectors.shopify_connection import sync_shopify_order
from erpnext.erpnext_integrations.doctype.shopify_settings.sync_payout import (
	Payouts, create_or_update_shopify_payout, get_shopify_document)
from frappe.model.document import Document


class ShopifyPayout(Document):
	def before_submit(self):
		self.update_shopify_payout()
		self.create_missing_orders()

	def on_submit(self):
		self.update_cancelled_orders()
		self.create_sales_returns()
		self.create_journal_entry()

	def update_shopify_payout(self):
		payout = Payouts.find(self.payout_id)
		create_or_update_shopify_payout(payout, payout_doc=self)

	def create_missing_orders(self):
		for transaction in self.transactions:
			shopify_order_id = transaction.source_order_id

			# create an order, invoice and delivery, if missing
			if shopify_order_id and not get_shopify_document("Sales Order", shopify_order_id):
				order = Order.find(shopify_order_id)
				sync_shopify_order(order.to_dict())

				transaction.update({
					"sales_order": get_shopify_document("Sales Order", shopify_order_id),
					"sales_invoice": get_shopify_document("Sales Invoice", shopify_order_id),
					"delivery_note": get_shopify_document("Delivery Note", shopify_order_id)
				})

	def update_cancelled_orders(self):
		for transaction in self.transactions:
			doctypes = ["Delivery Note", "Sales Invoice", "Sales Order"]
			for doctype in doctypes:
				doctype_field = frappe.scrub(doctype)
				docname = transaction.get(doctype_field)
				if docname:
					frappe.get_doc(doctype, docname).cancel()

	def create_sales_returns(self):
		invoices = {transaction.sales_invoice: transaction.source_order_id for transaction in self.transactions
			if transaction.sales_invoice and transaction.source_order_id}

		for sales_invoice_id, shopify_order_id in invoices.items():
			shopify_order = Order.find(shopify_order_id) or frappe._dict()

			is_order_refunded = shopify_order.financial_status == "refunded"
			is_invoice_returned = frappe.db.get_value("Sales Invoice", sales_invoice_id, "status") in ["Return",
				"Credit Note Issued"]

			if is_order_refunded and not is_invoice_returned:
				return_invoice = make_sales_return(sales_invoice_id)
				return_invoice.save()
				return_invoice.submit()

	def create_journal_entry(self):
		payouts_by_order = defaultdict(list)
		for transaction in self.transactions:
			if transaction.sales_invoice:
				payouts_by_order[transaction.sales_invoice].append(transaction)

		for invoice_id, order_transactions in payouts_by_order.items():
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
