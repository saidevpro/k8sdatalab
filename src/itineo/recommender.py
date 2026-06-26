from . import engine


def recommend(subscription, service_date):
    params = engine.params_from_subscription(subscription)
    return engine.search(params, service_date)
