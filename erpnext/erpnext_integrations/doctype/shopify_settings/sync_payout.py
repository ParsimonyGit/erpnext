from shopify import PaginatedIterator, ShopifyResource

import frappe
from erpnext.erpnext_integrations.doctype.shopify_log.shopify_log import make_shopify_log
from frappe.utils import flt, getdate, now


class Payouts(ShopifyResource):
	# temporary class until https://github.com/Shopify/shopify_python_api/pull/431 is merged
	_prefix_source = "/shopify_payments/"


class Transactions(ShopifyResource):
	# temporary class until https://github.com/Shopify/shopify_python_api/pull/431 is merged
	_prefix_source = "/shopify_payments/balance/"


def get_payouts(shopify_settings):
	kwargs = dict()
	if shopify_settings.last_sync_datetime:
		kwargs['date_min'] = shopify_settings.last_sync_datetime

	try:
		payouts = PaginatedIterator(Payouts.find(**kwargs))
	except Exception as e:
		make_shopify_log(status="Error", exception=e, rollback=True)
		return []
	else:
		return payouts


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

	with shopify_settings.get_shopify_session(temp=True):
		payouts = get_payouts(shopify_settings)
		frappe.enqueue(method=create_shopify_payouts, queue='long', **{"payouts": payouts})

	shopify_settings.last_sync_datetime = now()
	shopify_settings.save()
	return True


def create_shopify_payouts(payouts):
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


def create_or_update_shopify_payout(payout, payout_doc=None):
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
		make_shopify_log(status="Error", response_data=payout.to_dict(), exception=e)
		return

	for transaction in payout_transactions:
		shopify_order_id = transaction.source_order_id
		payout_doc.append("transactions", {
			"transaction_id": transaction.id,
			"transaction_type": frappe.unscrub(transaction.type),
			"processed_at": getdate(transaction.processed_at),
			"total_amount": flt(transaction.amount),
			"fee": flt(transaction.fee),
			"net_amount": flt(transaction.net),
			"currency": transaction.currency,
			"sales_order": get_shopify_document("Sales Order", shopify_order_id),
			"sales_invoice": get_shopify_document("Sales Invoice", shopify_order_id),
			"delivery_note": get_shopify_document("Delivery Note", shopify_order_id),
			"source_id": transaction.source_id,
			"source_type": frappe.unscrub(transaction.source_type),
			"source_order_id": shopify_order_id,
			"source_order_transaction_id": transaction.source_order_transaction_id,
		})

	payout_doc.save()
	return payout_doc.name
