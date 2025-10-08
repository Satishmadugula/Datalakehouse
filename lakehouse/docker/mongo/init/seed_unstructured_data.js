db = db.getSiblingDB(process.env.MONGO_DATABASE || "payments");

db.orders.drop();
db.orders.insertMany([
  {
    order_id: "ord-100",
    merchant_id: "m-1",
    status: "created",
    amount: 125.5,
    currency: "USD",
    event_ts: ISODate(),
    billing: {
      address: {
        line1: "123 Main St",
        city: "Austin",
        state: "TX"
      },
      card: {
        brand: "visa",
        last4: "1234"
      }
    },
    items: [
      { sku: "sku-1", quantity: 1 },
      { sku: "sku-2", quantity: 2 }
    ]
  },
  {
    order_id: "ord-101",
    merchant_id: "m-2",
    status: "approved",
    amount: 78.2,
    currency: "USD",
    event_ts: ISODate(),
    marketing: {
      campaign: "spring",
      coupon: "SPRING10"
    }
  }
]);
