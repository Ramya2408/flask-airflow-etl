from flask import Flask, jsonify
import psycopg2
from config import get_db_config

app = Flask(__name__)

@app.route('/spend/<customer_id>', methods=['GET'])
def spend(customer_id):
    cfg = get_db_config()
    sql = """
    SELECT customer_id,
           COUNT(*) AS orders,
           SUM(net_value)::numeric(10,2) AS total_net
      FROM orders
     WHERE customer_id = %s
  GROUP BY customer_id
    """
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    cur.execute(sql, (customer_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "No data"}), 404
    return jsonify({
      "customerId": row[0],
      "orders": row[1],
      "totalNetMerchandiseValueEur": float(row[2])
    })

if __name__ == '__main__':
    app.run(port=8080, debug=True)
