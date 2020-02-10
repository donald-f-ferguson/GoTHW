
def order_link(order):

    links = []
    links.append({"rel": "customer", "href": "/api/classicmodels/customers/" + str(order["customerNumber"])})
    links.append({"rel": "self", "href": "/api/classicmodels/orders/" + str(order["orderNumber"])})
    links.append({"rel": "orderdetails",
                  "href": "/api/classicmodels/orderdetails?orderNumber=" + str(order["orderNumber"])})
    order['links'] = links

    return order


def orders_links(orders):

    if type(orders) == list:

        result = []

        for o in orders:
            new_o = order_link(o)
            result.append(new_o)

    else:
        result = order_link(orders)

    return result


def cast_link(cast_member):
    url_base = "https://www.imdb.com"

    ll =  cast_member.get("actorLink",None)
    if ll is not None:
        full_url = url_base + ll
        links = [
            {"rel": "imdb", "href": full_url}
            ]
        cast_member["links"] = links

    return cast_member


def cast_links(cast):

    if type(cast) == list:

        result = []

        for c in cast:
            new_c = cast_link(c)
            result.append(new_c)

    else:
        result = cast_link(cast)

    return result



def add_links(dbname, tablename, rsp_data):

    if dbname == 'classicmodels' and tablename == 'orders':
        result = orders_links(rsp_data)
    elif dbname == 'W4111GoTSolutionClean' and tablename == 'actors_episodes':
        result = cast_links(rsp_data)
    else:
        result = rsp_data

    return result


