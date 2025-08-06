import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = '12345'

def connect_db():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password=password
    )

def create_view_completed_orders():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE OR REPLACE VIEW completed_orders AS
    SELECT *
    FROM orders
    WHERE status = 'completed';
    """)
            conn.commit()

def create_view_electronics_products():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE OR REPLACE VIEW electronics_products AS
SELECT *
FROM products
WHERE category = 'Electronics';
""")
            conn.commit()

def total_spending_per_customer():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""WITH total_spent AS (
    SELECT o.customer_id,
           SUM(p.price * o.quantity) AS total_amount
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    WHERE o.status = 'completed'
    GROUP BY o.customer_id
)
SELECT c.full_name, t.total_amount
FROM customers c
LEFT JOIN total_spent t ON c.customer_id = t.customer_id;

""")
            return cur.fetchall()

def order_details_with_total():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""WITH order_details AS (
    SELECT o.order_id,
           c.full_name,
           p.product_name,
           o.quantity,
           p.price,
           (o.quantity * p.price) AS total_price
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN products p ON o.product_id = p.product_id
)
SELECT * FROM order_details;

            """)
            return cur.fetchall()

def get_customer_who_bought_most_expensive_product():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT c.full_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
ORDER BY p.price DESC
LIMIT 1;

""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 2. Sipariş durumlarına göre açıklama
def get_order_status_descriptions():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT order_id,
       status,
       CASE 
            WHEN status = 'completed' THEN 'Tamamlandı'
            WHEN status = 'cancelled' THEN 'İptal Edildi'
       END AS status_description
FROM orders;

""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 3. Ortalama fiyatın üstündeki ürünler
def get_products_above_average_price():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT product_name, price
FROM products
WHERE price > (SELECT AVG(price) FROM products);

""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 4. Müşteri kategorileri
def get_customer_categories():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT c.full_name,
       CASE
            WHEN COUNT(o.order_id) > 5 THEN 'Sadık Müşteri'
            WHEN COUNT(o.order_id) BETWEEN 2 AND 5 THEN 'Orta Seviye'
            ELSE 'Yeni Müşteri'
       END AS customer_category
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.full_name;

""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 5. Son 30 gün içinde sipariş veren müşteriler
def get_recent_customers():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT c.full_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days';

""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 6. En çok sipariş verilen ürün
def get_most_ordered_product():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT p.product_name, SUM(o.quantity) AS total_ordered
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_ordered DESC
LIMIT 1;
""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 7. Ürün fiyatlarına göre etiketleme
def get_product_price_categories():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT product_name,
       price,
       CASE
            WHEN price > 1000 THEN 'Pahalı'
            WHEN price BETWEEN 100 AND 500 THEN 'Orta'
            ELSE 'Ucuz'
       END AS price_category
FROM products;
""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result