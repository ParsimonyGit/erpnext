from shopify import ShopifyResource

import frappe
from erpnext.erpnext_integrations.doctype.shopify_log.shopify_log import make_shopify_log
from frappe.utils import now


class Payouts(ShopifyResource):
	# temporary class until https://github.com/Shopify/shopify_python_api/pull/431 is merged
	_prefix_source = "/shopify_payments/"


class Transactions(ShopifyResource):
	# temporary class until https://github.com/Shopify/shopify_python_api/pull/431 is merged
	_prefix_source = "/shopify_payments/balance/"


def get_payouts(shopify_settings):
	# kwargs = dict()
	# if shopify_settings.last_sync_datetime:
	# 	kwargs['date_min'] = shopify_settings.last_sync_datetime

	try:
		# payouts = Payouts.find(**kwargs)
		payouts = Payouts.find()
	except Exception as e:
		make_shopify_log(status="Error", exception=e, rollback=True)
		return []
	else:
		return payouts


def get_shopify_invoice(order_id):
	return frappe.db.get_value("Sales Invoice", {"docstatus": 1, "shopify_order_id": order_id}, "name")


@frappe.whitelist()
def sync_payout_from_shopify():
	"""
	Pull and sync payouts from Shopify Payments transactions with existing orders
	"""

	shopify_settings = frappe.get_single("Shopify Settings")
	if not shopify_settings.enable_shopify:
		return

	with shopify_settings.get_shopify_session(temp=True):
		payouts = get_payouts(shopify_settings)
		for payout in payouts:
			try:
				payout_transactions = Transactions.find(payout_id=payout.id)
			except Exception as e:
				make_shopify_log(status="Error", exception=e, rollback=True)
				continue

			for transaction in payout_transactions:
				invoice = get_shopify_invoice(transaction.source_order_id)

				if not invoice:
					# TODO: should we create a new invoice for this payout?
					pass
				else:
					# TODO: add the fees and charges to the existing invoice
					pass

	shopify_settings.last_sync_datetime = now()
	shopify_settings.save()
