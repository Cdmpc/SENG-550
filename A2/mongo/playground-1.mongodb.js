// MongoDB Playground
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.

// The current database to use.
use("sales_db");

/* 1. Count the total cities each customer in the collection has lived in. */
db.getCollection("orders_summary").aggregate([
  {
    // GROUP BY customer_id, customer_city
    $group: {
      _id: "$customer_id",
      customer_name: { $addToSet: "$customer_name" },
      cities: { $addToSet: "$customer_city" },
    },
  },
  {
    // Pulls the customer name field to the top level view.
    $unwind: "$customer_name",
  },
  {
    // SELECT customer_name, customer_city FROM orders_summary
    $project: {
      customer_name: 1,
      cities: 1,
      // COUNT(cities) AS number_of_cities
      number_of_cities: { $size: "$cities" },
    },
  },
]);

/** 2. Get total amount SOLD per city. */
use("sales_db");
db.getCollection("orders_summary").aggregate([
  {
    $group: {
      _id: {
        customer_id: "$customer_id",
        customer_name: "$customer_name",
        customer_city: "$customer_city",
      },
      total_amount: {
        $sum: "$amount",
      },
    },
  },
]);

/** 3. Get the sum of discounts between actual amount paid and the original price of the order. */
use("sales_db");
db.getCollection("orders_summary").aggregate([
  {
    $group: {
      _id: {
        product_id: "$product_id",
        product_name: "$product_name",
      },
      // SUM((price - amount)) AS sum_of_discounts
      sum_of_discounts: {
        $sum: {
          $subtract: ["$product_price", "$amount"],
        },
      },
    },
  },
  {
    $project: {
      product_id: "$product_id",
      product_name: "$product_name",
      sum_of_discounts: "$sum_of_discounts",
    },
  },
]);

/* 4. Return all orders for a specific customer, including order_id,
    order_date, customer_city (at the time of order), product_name, product_price, amount */
use("sales_db");
db.getCollection("orders_summary").aggregate([
  {
    $match: {
      customer_id: 1,
    },
  },
  {
    $project: {
      customer_name: 1,
      product_name: 1,
      order_id: 1,
      order_date: 1,
      customer_city: 1,
      product_price: 1,
      amount: 1,
      _id: 0,
    },
  },
]);
