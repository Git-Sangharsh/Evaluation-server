import express from 'express';
import mongoose from 'mongoose';
import cors from "cors";
import bodyParser from 'body-parser';
// MongoDB connection URI
const MONGO_URI = 'mongodb+srv://db_user_read:LdmrVA5EDEv4z3Wr@cluster0.n10ox.mongodb.net/RQ_Analytics?retryWrites=true&w=majority&appName=Cluster0';
const PORT = 5000;

// Initialize Express
const app = express();
app.use(cors());
app.use(bodyParser.json());

// Define Mongoose Schemas and Models
const customerSchema = new mongoose.Schema({
  first_name: String,
  last_name: String,
  email: String,
  created_at: Date,
  orders_count: Number,
  total_spent: Number,
  default_address: {
    city: String,
    province: String,
    country: String,
  },
});

const orderSchema = new mongoose.Schema({
  total_price_set: {
    shop_money: {
      amount: String,
      currency_code: String,
    },
  },
  created_at: Date,
});

const productSchema = new mongoose.Schema({
  title: String,
  vendor: String,
  created_at: Date,
});

const Customer = mongoose.model('Customer', customerSchema, 'shopifyCustomers');
const Order = mongoose.model('Order', orderSchema, 'shopifyOrders');
const Product = mongoose.model('Product', productSchema, 'shopifyProducts');

// Helper function to determine grouping interval
const getGroupingInterval = (interval) => {
  switch (interval) {
    case 'daily':
      return { $dayOfMonth: '$created_at' };
    case 'monthly':
      return { $month: '$created_at' };
    case 'quarterly':
      return { $ceil: { $divide: [{ $month: '$created_at' }, 3] } };
    case 'yearly':
      return { $year: '$created_at' };
    default:
      throw new Error('Invalid interval');
  }
};

// Routes
app.get('/api/analytics/sales', async (req, res) => {
  try {
    const { interval } = req.query;
    const groupBy = getGroupingInterval(interval);

    const sales = await Order.aggregate([
      {
        $project: {
          created_at: {
            $dateFromString: { dateString: '$created_at' }
          },
          total_price_set: 1
        },
      },
      {
        $group: {
          _id: groupBy,
          totalSales: { $sum: { $toDouble: '$total_price_set.shop_money.amount' } },
        },
      },
    ]);

    res.json(sales);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/analytics/sales-growth', async (req, res) => {
  try {
    const { interval } = req.query;
    const groupBy = getGroupingInterval(interval);

    const salesGrowth = await Order.aggregate([
      {
        $project: {
          created_at: {
            $dateFromString: { dateString: '$created_at' }
          },
          total_price_set: 1
        },
      },
      {
        $group: {
          _id: groupBy,
          totalSales: { $sum: { $toDouble: '$total_price_set.shop_money.amount' } },
        },
      },
      {
        $sort: { _id: 1 }
      },
      {
        $bucket: {
          groupBy: '$_id',
          boundaries: [0, 1, 2, 3, 4],
          default: 'Other',
          output: {
            totalSales: { $sum: '$totalSales' }
          }
        }
      }
    ]);

    res.json(salesGrowth);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/analytics/new-customers', async (req, res) => {
  try {
    const { interval } = req.query;
    const groupBy = getGroupingInterval(interval);

    const newCustomers = await Customer.aggregate([
      {
        $project: {
          created_at: {
            $dateFromString: { dateString: '$created_at' }
          }
        },
      },
      {
        $group: {
          _id: groupBy,
          count: { $sum: 1 },
        },
      },
    ]);

    res.json(newCustomers);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/analytics/repeat-customers', async (req, res) => {
  try {
    const { interval } = req.query;
    const groupBy = getGroupingInterval(interval);

    const repeatCustomers = await Order.aggregate([
      {
        $addFields: {
          created_at: {
            $dateFromString: { dateString: '$created_at' }
          }
        }
      },
      {
        $group: {
          _id: {
            customerId: '$customer_id',
            interval: groupBy
          },
          orderCount: { $sum: 1 }
        }
      },
      {
        $match: { orderCount: { $gt: 1 } }
      },
      {
        $group: {
          _id: '$_id.interval',
          repeatCustomersCount: { $sum: 1 }
        }
      }
    ]);

    res.json(repeatCustomers);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/analytics/geographical-distribution', async (req, res) => {
  try {
    const distribution = await Customer.aggregate([
      {
        $group: {
          _id: '$default_address.city',
          count: { $sum: 1 },
        },
      },
    ]);

    res.json(distribution);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/analytics/customer-lifetime-value', async (req, res) => {
  try {
    const cohorts = await Customer.aggregate([
      {
        $project: {
          created_at: {
            $dateFromString: { dateString: '$created_at' }
          },
          total_spent: 1,
        },
      },
      {
        $group: {
          _id: { $month: '$created_at' },
          lifetimeValue: { $sum: '$total_spent' },
        },
      },
    ]);

    res.json(cohorts);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Connect to MongoDB and start the server
mongoose
  .connect(MONGO_URI)
  .then(() => {
    console.log('Connected to MongoDB');
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  })
  .catch((error) => {
    console.error('Connection error', error.message);
  });
