from shopify import Order, PaginatedIterator, Payouts, Transactions

import frappe
from erpnext.erpnext_integrations.doctype.shopify_log.shopify_log import make_shopify_log
from frappe.utils import flt, getdate, now


def get_payouts(shopify_settings):
	kwargs = dict()
	# if shopify_settings.last_sync_datetime:
	# 	kwargs['date_min'] = shopify_settings.last_sync_datetime

	session = shopify_settings.get_shopify_session()
	Payouts.activate_session(session)

	try:
		payouts = PaginatedIterator(Payouts.find(**kwargs))
	except Exception as e:
		make_shopify_log(status="Payout Error", exception=e, rollback=True)
		return []
	else:
		return payouts
	finally:
		Payouts.clear_session()


def get_shopify_document(doctype, shopify_order_id):
	return frappe.db.get_value(doctype,
		{"docstatus": 1, "shopify_order_id": shopify_order_id}, "name")


@frappe.whitelist()
def sync_payouts_from_shopify():
	"""
	Pull and sync payouts from Shopify Payments transactions with existing orders
	"""

	shopify_settings = frappe.get_single("Shopify Settings")
	if not shopify_settings.enable_shopify:
		return

	payouts = get_payouts(shopify_settings)
	create_shopify_payouts(payouts)
	# frappe.enqueue(method=create_shopify_payouts, queue='long', **{"payouts": payouts})

	shopify_settings.last_sync_datetime = now()
	shopify_settings.save()
	return True


def create_shopify_payouts(payouts):
	settings = frappe.get_single("Shopify Settings")
	session = settings.get_shopify_session()

	Payouts.activate_session(session)
	Transactions.activate_session(session)
	Order.activate_session(session)

	for page in payouts:
		for payout in page:
			payout_docstatus = frappe.db.get_value("Shopify Payout", {"payout_id": payout.id}, "docstatus")

			# skip payout generation if one is already submitted or cancelled,
			# and update an existing draft payout if it exists
			if payout_docstatus is None:
				create_or_update_shopify_payout(payout)
			elif payout_docstatus == 0:
				payout_name = frappe.db.get_value("Shopify Payout", {"payout_id": payout.id}, "name")
				payout_doc = frappe.get_doc("Shopify Payout", payout_name)
				create_or_update_shopify_payout(payout, payout_doc)

	Payouts.clear_session()
	Transactions.clear_session()
	Order.clear_session()


def create_or_update_shopify_payout(payout, payout_doc=None):
	"""
	Create a Payout document from Shopify's Payout information.
	If a payout exists, update that instead.

	Args:

		payout (Payout): The Payout payload from Shopify
		payout_doc (ShopifyPayout, optional): The existing Shopify Payout ERPNext
			document. Defaults to None.

	Returns:

		str: The document ID of the created / updated Shopify Payout
	"""

	if not payout_doc:
		payout_doc = frappe.new_doc("Shopify Payout")

	company = frappe.db.get_single_value("Shopify Settings", "company")
	payout_doc.update({
		"company": company,
		"payout_id": payout.id,
		"payout_date": getdate(payout.date),
		"status": frappe.unscrub(payout.status),
		"amount": flt(payout.amount),
		"currency": payout.currency,
		**payout.summary.to_dict()  # unpack the payout amounts and fees from the summary
	})

	try:
		payout_transactions = Transactions.find(payout_id=payout.id)
	except Exception as e:
		payout_doc.save()
		make_shopify_log(status="Payout Transactions Error", response_data=payout.to_dict(), exception=e)
		return payout_doc.name

	payout_doc.set("transactions", [])
	for transaction in payout_transactions:
		shopify_order_id = transaction.source_order_id

		order_financial_status = None
		if shopify_order_id:
			order = Order.find(shopify_order_id)
			order_financial_status = frappe.unscrub(order.financial_status)

		total_amount = -flt(transaction.amount) if transaction.type == "payout" else flt(transaction.amount)
		net_amount = -flt(transaction.net) if transaction.type == "payout" else flt(transaction.net)

		payout_doc.append("transactions", {
			"transaction_id": transaction.id,
			"transaction_type": frappe.unscrub(transaction.type),
			"processed_at": getdate(transaction.processed_at),
			"total_amount": total_amount,
			"fee": flt(transaction.fee),
			"net_amount": net_amount,
			"currency": transaction.currency,
			"sales_order": get_shopify_document("Sales Order", shopify_order_id),
			"sales_invoice": get_shopify_document("Sales Invoice", shopify_order_id),
			"delivery_note": get_shopify_document("Delivery Note", shopify_order_id),
			"source_id": transaction.source_id,
			"source_type": frappe.unscrub(transaction.source_type),
			"source_order_financial_status": order_financial_status,
			"source_order_id": shopify_order_id,
			"source_order_transaction_id": transaction.source_order_transaction_id,
		})

	payout_doc.save()
	frappe.db.commit()
	return payout_doc.name
