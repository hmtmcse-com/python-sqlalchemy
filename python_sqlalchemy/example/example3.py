import asyncio
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, func, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, relationship, aliased
from sqlalchemy.future import select
from sqlalchemy import or_, and_, text, outerjoin  # Added outerjoin

from datetime import datetime

# Database URL for async SQLite
DATABASE_URL = "sqlite+aiosqlite:///./async_example.db"

Base = declarative_base()

async_engine = create_async_engine(DATABASE_URL, echo=True)


async def init_db():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# --- Model Definitions ---
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)

    # Relationship to Order (One-to-Many)
    orders = relationship("Order", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    price = Column(Integer, nullable=False)  # Storing price as integer for simplicity (e.g., cents)
    stock = Column(Integer, default=0)

    # Many-to-Many relationship with Order through OrderProduct
    order_associations = relationship("OrderProduct", back_populates="product", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Product(id={self.id}, name='{self.name}', price={self.price})>"


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    order_date = Column(DateTime, default=datetime.now)
    total_amount = Column(Integer, default=0)  # Storing total_amount as integer

    # Relationship to User (Many-to-One)
    user = relationship("User", back_populates="orders")

    # Many-to-Many relationship with Product through OrderProduct
    product_associations = relationship("OrderProduct", back_populates="order", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Order(id={self.id}, user_id={self.user_id}, total_amount={self.total_amount})>"


class OrderProduct(Base):
    __tablename__ = "order_products"
    order_id = Column(Integer, ForeignKey("orders.id"), primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id"), primary_key=True)
    quantity = Column(Integer, default=1)

    order = relationship("Order", back_populates="product_associations")
    product = relationship("Product", back_populates="order_associations")

    def __repr__(self):
        return f"<OrderProduct(order_id={self.order_id}, product_id={self.product_id}, quantity={self.quantity})>"


# --- CRUD Operations ---
async def create_items(session: AsyncSession):
    print("\n--- Creating Items ---")
    # Single User Create
    user1 = User(username="alice", email="alice@example.com")
    session.add(user1)
    await session.flush()  # Flush to get ID for user1 if needed immediately

    # Bulk User Create
    users_data = [
        {"username": "bob", "email": "bob@example.com"},
        {"username": "charlie", "email": "charlie@example.com"},
        {"username": "diana", "email": "diana@example.com", "is_active": False}
    ]
    new_users = [User(**data) for data in users_data]
    session.add_all(new_users)

    # Single Product Create
    product1 = Product(name="Laptop", price=120000, stock=50)  # price in cents
    session.add(product1)
    await session.flush()

    # Bulk Product Create
    products_data = [
        {"name": "Mouse", "price": 2500, "stock": 200},
        {"name": "Keyboard", "price": 7500, "stock": 100},
        {"name": "Monitor", "price": 25000, "stock": 30},
        {"name": "Webcam", "price": 3000, "stock": 150},
        {"name": "Speakers", "price": 4000, "stock": 80}
    ]
    new_products = [Product(**data) for data in products_data]
    session.add_all(new_products)

    await session.commit()
    print("Created initial users and products.")

    # Create an order for user1 with product1 and product2 (Mouse)
    # Fetch product2 (Mouse) by name since we don't have its ID directly from bulk creation
    product2_stmt = select(Product).where(Product.name == "Mouse")
    product2_result = await session.execute(product2_stmt)
    product2 = product2_result.scalar_one()

    order1 = Order(user_id=user1.id, total_amount=0)
    session.add(order1)
    await session.flush()

    order_product1 = OrderProduct(order_id=order1.id, product_id=product1.id, quantity=2)
    order_product2 = OrderProduct(order_id=order1.id, product_id=product2.id, quantity=1)
    session.add_all([order_product1, order_product2])
    await session.commit()

    # Update total_amount for order1 after products are added
    order1.total_amount = (2 * product1.price) + (1 * product2.price)
    await session.commit()
    print(f"Created order {order1.id} for user {user1.username}.")

    # Create another order for charlie
    charlie_stmt = select(User).where(User.username == "charlie")
    charlie_result = await session.execute(charlie_stmt)
    charlie = charlie_result.scalar_one()

    monitor_stmt = select(Product).where(Product.name == "Monitor")
    monitor_result = await session.execute(monitor_stmt)
    monitor = monitor_result.scalar_one()

    keyboard_stmt = select(Product).where(Product.name == "Keyboard")
    keyboard_result = await session.execute(keyboard_stmt)
    keyboard = keyboard_result.scalar_one()

    order2 = Order(user_id=charlie.id, total_amount=0)
    session.add(order2)
    await session.flush()

    order_product3 = OrderProduct(order_id=order2.id, product_id=monitor.id, quantity=1)
    order_product4 = OrderProduct(order_id=order2.id, product_id=keyboard.id, quantity=1)
    session.add_all([order_product3, order_product4])
    await session.commit()

    order2.total_amount = (1 * monitor.price) + (1 * keyboard.price)
    await session.commit()
    print(f"Created order {order2.id} for user {charlie.username}.")


async def update_items(session: AsyncSession):
    print("\n--- Updating Items ---")
    # Single Item Update
    stmt = select(User).where(User.username == "alice")
    result = await session.execute(stmt)
    alice = result.scalar_one_or_none()
    if alice:
        alice.email = "alice.new@example.com"
        print(f"Updated Alice's email to: {alice.email}")

    # Bulk Update (using synchronize_session=False for performance with WHERE clause)
    # Deactivate all users whose username starts with 'b'
    # Note: synchronize_session=False is for direct table updates.
    # When updating via ORM objects, SQLAlchemy manages session synchronization.
    await session.execute(
        User.__table__.update()
        .where(User.username.like("b%"))
        .values(is_active=False)
    )
    print("Deactivated users whose username starts with 'b'.")

    # Another way for bulk update, fetching and then modifying
    stmt = select(Product).where(Product.stock < 100)
    result = await session.execute(stmt)
    low_stock_products = result.scalars().all()
    for product in low_stock_products:
        product.stock += 50  # Increase stock by 50
    print(f"Updated stock for {len(low_stock_products)} low-stock products.")

    await session.commit()


# --- Listing and Pagination ---
async def list_items_without_pagination(session: AsyncSession):
    print("\n--- Listing All Users (No Pagination) ---")
    stmt = select(User)
    result = await session.execute(stmt)
    users = result.scalars().all()
    for user in users:
        print(user)


async def list_items_with_pagination(session: AsyncSession, model, page: int = 1, per_page: int = 10):
    print(f"\n--- Listing {model.__tablename__} (Page {page}, Per Page {per_page}) ---")
    offset = (page - 1) * per_page
    stmt = select(model).offset(offset).limit(per_page)
    result = await session.execute(stmt)
    items = result.scalars().all()

    # Get total count for pagination info
    total_stmt = select(func.count()).select_from(model)
    total_count_result = await session.execute(total_stmt)
    total_count = total_count_result.scalar_one()

    print(f"Total {model.__tablename__}: {total_count}")
    for item in items:
        print(item)
    return items, total_count


# --- Alias and Label ---
async def alias_and_label_example(session: AsyncSession):
    print("\n--- Alias and Label Example ---")

    # Example: Find users who created orders, showing user's username and order's total amount
    UserAlias = aliased(User)
    stmt = select(
        UserAlias.username.label("customer_username"),  # Labeling a column
        Order.total_amount
    ).join(UserAlias, UserAlias.id == Order.user_id)  # Join Order to the aliased User

    result = await session.execute(stmt)
    for row in result:
        print(f"Customer Username: {row.customer_username}, Order Total: {row.total_amount / 100:.2f}")

    # Example with a label for an aggregated column
    stmt = select(
        Product.name,
        (Product.price * Product.stock).label("total_inventory_value")  # Labeling an expression
    ).where(Product.stock > 0)

    result = await session.execute(stmt)
    for row in result:
        print(f"Product: {row.name}, Total Inventory Value: {row.total_inventory_value / 100:.2f}")


# --- Relationships with Conditions and Sorting ---
async def relationship_with_conditions_and_sorting(session: AsyncSession):
    print("\n--- Relationship with Conditions and Sorting Example ---")

    # Fetch user 'alice' and her orders, but only orders with total_amount > 50000, sorted by date
    stmt = select(User).where(User.username == "alice")
    result = await session.execute(stmt)
    alice = result.scalar_one_or_none()

    if alice:
        print(f"Alice's orders (total_amount > $500.00, sorted by date):")
        # Note: Filtering/sorting on loaded relationships happens in Python,
        # for database-level filtering, use a join as shown below.
        filtered_sorted_orders = sorted(
            [o for o in alice.orders if o.total_amount > 50000],
            key=lambda x: x.order_date,
            reverse=True
        )
        for order in filtered_sorted_orders:
            print(f"  - {order} (Total: {order.total_amount / 100:.2f})")

    # More direct way with JOIN and ORDER BY for database-level filtering/sorting
    print("\nOrders with total_amount > $500.00, sorted by date (database-level):")
    stmt = select(User.username, Order.id, Order.order_date, Order.total_amount).join(User.orders).where(
        Order.total_amount > 50000).order_by(Order.order_date.desc())
    result = await session.execute(stmt)
    for username, order_id, order_date, total_amount in result:
        print(
            f"  User: {username}, Order ID: {order_id}, Date: {order_date.strftime('%Y-%m-%d %H:%M:%S')}, Total: {total_amount / 100:.2f}")


# --- Aggregation Examples ---
async def aggregation_examples(session: AsyncSession):
    print("\n--- Aggregation Examples ---")

    # Total number of users
    total_users_stmt = select(func.count(User.id))
    total_users = (await session.execute(total_users_stmt)).scalar_one()
    print(f"Total number of users: {total_users}")

    # Total stock of all products
    total_stock_stmt = select(func.sum(Product.stock))
    total_stock = (await session.execute(total_stock_stmt)).scalar_one()
    print(f"Total stock of all products: {total_stock}")

    # Average product price
    avg_price_stmt = select(func.avg(Product.price))
    avg_price = (await session.execute(avg_price_stmt)).scalar_one()
    print(f"Average product price: {avg_price / 100:.2f}")  # Convert back from cents

    # Maximum order total
    max_order_stmt = select(func.max(Order.total_amount))
    max_order_total = (await session.execute(
        max_order_stmt)).scalar_one_or_none()  # Use scalar_one_or_none for potentially empty results
    print(f"Maximum order total: {max_order_total / 100:.2f}" if max_order_total is not None else "No orders found.")

    # Count of orders per user (GROUP BY)
    print("\nOrders per user:")
    orders_per_user_stmt = select(
        User.username,
        func.count(Order.id).label("order_count")
    ).join(Order, User.id == Order.user_id).group_by(User.username).order_by(User.username)
    result = await session.execute(orders_per_user_stmt)
    for username, order_count in result:
        print(f"  {username}: {order_count} orders")

    # Products with stock greater than average stock (using scalar subquery)
    avg_product_stock_subquery = select(func.avg(Product.stock)).scalar_subquery()
    stmt = select(Product.name, Product.stock).where(Product.stock > avg_product_stock_subquery)
    result = await session.execute(stmt)
    print("\nProducts with stock greater than average stock:")
    for name, stock in result:
        print(f"  {name}: {stock}")


# --- Filtering Examples ---
async def filtering_examples(session: AsyncSession):
    print("\n--- Filtering Examples ---")

    # Equal
    print("\nUsers with username 'alice':")
    stmt = select(User).where(User.username == "alice")
    result = await session.execute(stmt)
    for user in result.scalars():
        print(user)

    # Less Than
    print("\nProducts with price less than $50:")
    stmt = select(Product).where(Product.price < 5000)  # Less than 5000 cents
    result = await session.execute(stmt)
    for product in result.scalars():
        print(product)

    # Greater Than
    print("\nProducts with stock greater than 100:")
    stmt = select(Product).where(Product.stock > 100)
    result = await session.execute(stmt)
    for product in result.scalars():
        print(product)

    # BETWEEN
    print("\nProducts with price between $10 and $100:")
    stmt = select(Product).where(Product.price.between(1000, 10000))  # Between 1000 and 10000 cents
    result = await session.execute(stmt)
    for product in result.scalars():
        print(product)

    # AND
    print("\nActive users with 'example.com' in their email:")
    stmt = select(User).where(and_(User.is_active == True, User.email.like("%example.com%")))
    result = await session.execute(stmt)
    for user in result.scalars():
        print(user)

    # OR
    print("\nUsers named 'alice' OR 'charlie':")
    stmt = select(User).where(or_(User.username == "alice", User.username == "charlie"))
    result = await session.execute(stmt)
    for user in result.scalars():
        print(user)

    # IS NULL / IS NOT NULL (example with a hypothetical nullable column)
    # If Product had a 'description' Column(String, nullable=True):
    # stmt = select(Product).where(Product.description.is_(None))
    # stmt = select(Product).where(Product.description.isnot(None))


# --- Join Examples ---
async def join_examples(session: AsyncSession):
    print("\n--- Join Examples ---")

    # Inner Join (Implicit via relationship)
    print("\nUsers and their orders (Inner Join - implicit via relationship):")
    stmt = select(User.username, Order.id, Order.total_amount).join(User.orders)
    result = await session.execute(stmt)
    for username, order_id, total_amount in result:
        print(f"  User: {username}, Order ID: {order_id}, Total: {total_amount / 100:.2f}")

    # Inner Join (Explicit)
    print("\nUsers and their orders (Inner Join - explicit):")
    stmt = select(User.username, Order.id).join(Order, User.id == Order.user_id)
    result = await session.execute(stmt)
    for username, order_id in result:
        print(f"  User: {username}, Order ID: {order_id}")

    # Left Join (LEFT OUTER JOIN) - Users and their orders (even if they have no orders)
    print("\nUsers and their orders (Left Join):")
    stmt = select(User.username, Order.id, Order.total_amount).outerjoin(Order, User.id == Order.user_id)
    result = await session.execute(stmt)
    for username, order_id, total_amount in result:
        print(
            f"  User: {username}, Order ID: {order_id if order_id else 'N/A'}, Total: {total_amount / 100:.2f}" if total_amount is not None else f"  User: {username}, No orders")

    # Full Outer Join (Emulation for SQLite)
    # SQLAlchemy's `outerjoin(..., full=True)` performs a full outer join if the backend supports it.
    # For SQLite, it's often a UNION of LEFT JOIN and RIGHT JOIN (simulated).
    print("\nProducts and their associated orders (Full Outer Join emulation for SQLite):")
    # To demonstrate a 'full outer join', we'll ensure a product exists without an order, and an order exists without a specific product type (if possible, though our schema makes this hard)
    # For a true full outer join effect, you might need more complex data or a database that natively supports it.
    # Here, we'll show how to use outerjoin for both sides.
    stmt = select(
        Product.name,
        Order.id.label("order_id")
    ).outerjoin(OrderProduct, Product.id == OrderProduct.product_id) \
        .outerjoin(Order, OrderProduct.order_id == Order.id)  # Chained outer joins

    result = await session.execute(stmt)
    for product_name, order_id in result:
        print(f"  Product: {product_name if product_name else 'N/A'}, Order ID: {order_id if order_id else 'N/A'}")


# --- Subquery Examples ---
async def subquery_examples(session: AsyncSession):
    print("\n--- Subquery Examples ---")

    # Subquery in WHERE clause (Find users who have placed at least one order)
    print("\nUsers who have placed at least one order (using EXISTS subquery):")
    exists_stmt = select(Order.user_id).where(Order.user_id == User.id).exists()
    stmt = select(User).where(exists_stmt)
    result = await session.execute(stmt)
    for user in result.scalars():
        print(user)

    # Subquery in FROM clause (Derived table) - Find average order amount for users with more than 1 order
    print("\nAverage order amount for users with more than one order (using subquery as a derived table):")
    subquery_orders_per_user = select(
        Order.user_id,
        func.count(Order.id).label("order_count"),
        func.avg(Order.total_amount).label("avg_order_amount")
    ).group_by(Order.user_id).having(func.count(Order.id) > 1).subquery()

    stmt = select(
        User.username,
        subquery_orders_per_user.c.order_count,
        subquery_orders_per_user.c.avg_order_amount
    ).join(subquery_orders_per_user, User.id == subquery_orders_per_user.c.user_id)

    result = await session.execute(stmt)
    for username, order_count, avg_amount in result:
        print(f"  User: {username}, Orders: {order_count}, Avg Order Amount: {avg_amount / 100:.2f}")

    # Scalar Subquery in SELECT clause (Get each user's latest order total)
    print("\nEach user's latest order total (using scalar subquery):")
    # For a true "latest" order, you might need a more robust ordering (e.g., order_date and then id for tie-breaking)
    latest_order_subquery = select(Order.total_amount) \
        .where(Order.user_id == User.id) \
        .order_by(Order.order_date.desc(), Order.id.desc()) \
        .limit(1) \
        .scalar_subquery()
    stmt = select(User.username, latest_order_subquery.label("latest_order_total"))
    result = await session.execute(stmt)
    for username, latest_total in result:
        print(
            f"  User: {username}, Latest Order Total: {latest_total / 100:.2f}" if latest_total is not None else f"  User: {username}, No orders")


# --- Text Based Query Running ---
async def text_based_query_example(session: AsyncSession):
    print("\n--- Text Based Query Running ---")

    # Executing a raw SELECT query
    print("\nAll users (raw SQL):")
    stmt = text("SELECT id, username, email FROM users")
    result = await session.execute(stmt)
    for row in result:
        print(f"  ID: {row.id}, Username: {row.username}, Email: {row.email}")

    # Executing a raw UPDATE query
    print("\nUpdating 'bob' to active using raw SQL:")
    await session.execute(text("UPDATE users SET is_active = :is_active WHERE username = :username"),
                          {"is_active": True, "username": "bob"})
    await session.commit()
    print("Updated 'bob' to active.")

    # Fetching 'bob' to confirm
    stmt = select(User).where(User.username == "bob")
    result = await session.execute(stmt)
    bob = result.scalar_one_or_none()
    if bob:
        print(f"Bob after raw update: {bob.is_active}")

    # Executing a raw DELETE query
    print("\nDeleting 'diana' using raw SQL:")
    await session.execute(text("DELETE FROM users WHERE username = 'diana'"))
    await session.commit()
    print("Deleted 'diana'.")

    # Confirm 'diana' is gone
    stmt = select(User).where(User.username == "diana")
    result = await session.execute(stmt)
    diana = result.scalar_one_or_none()
    print(f"Diana exists after delete: {diana is not None}")


# --- Main Execution ---
async def main():
    print("Initializing database...")
    await init_db()
    print("Database initialized.")

    # Use a single session for all operations in main for simplicity
    async_session_factory = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=async_engine,
        class_=AsyncSession
    )

    async with async_session_factory() as session:
        # Create
        await create_items(session)

        # Update
        await update_items(session)

        # List (no pagination)
        await list_items_without_pagination(session)

        # List (with pagination)
        await list_items_with_pagination(session, User, page=1, per_page=2)
        await list_items_with_pagination(session, Product, page=1, per_page=3)

        # Alias and Label
        await alias_and_label_example(session)

        # Relationship with conditions/sorting
        await relationship_with_conditions_and_sorting(session)

        # Aggregation
        await aggregation_examples(session)

        # Filtering
        await filtering_examples(session)

        # Joins
        await join_examples(session)

        # Subquery
        await subquery_examples(session)

        # Text Based Query
        await text_based_query_example(session)

    print("\n--- All operations completed ---")


if __name__ == "__main__":
    asyncio.run(main())
