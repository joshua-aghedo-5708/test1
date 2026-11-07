// Aggregation pipelines run by run_nosql.py (mongosh syntax)
db.transactions.aggregate([
  {
    "$match": {
      "status": "COMPLETED",
      "created_at": {
        "$gte": "2026-09-01 00:00:00",
        "$lt": "2026-10-01 00:00:00"
      }
    }
  },
  {
    "$group": {
      "_id": {
        "day": {
          "$dateTrunc": {
            "date": "$created_at",
            "unit": "day"
          }
        },
        "channel": "$channel"
      },
      "n": {
        "$sum": 1
      },
      "value": {
        "$sum": "$amount"
      }
    }
  },
  {
    "$sort": {
      "_id.day": 1,
      "_id.channel": 1
    }
  }
])

db.transactions.aggregate([
  {
    "$match": {
      "created_at": {
        "$gte": "2026-08-01 00:00:00",
        "$lt": "2026-10-01 00:00:00"
      },
      "status": {
        "$in": [
          "COMPLETED",
          "REVERSED"
        ]
      }
    }
  },
  {
    "$unwind": "$legs"
  },
  {
    "$match": {
      "legs.direction": "D",
      "legs.customer_id": {
        "$ne": 1
      }
    }
  },
  {
    "$group": {
      "_id": "$legs.customer_id",
      "outflow": {
        "$sum": "$legs.amount"
      },
      "debits": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "outflow": -1
    }
  },
  {
    "$limit": 10
  },
  {
    "$lookup": {
      "from": "customers",
      "localField": "_id",
      "foreignField": "_id",
      "as": "c",
      "pipeline": [
        {
          "$project": {
            "name": 1,
            "address.region": 1
          }
        }
      ]
    }
  },
  {
    "$project": {
      "outflow": 1,
      "debits": 1,
      "name": {
        "$first": "$c.name"
      },
      "region": {
        "$first": "$c.address.region"
      }
    }
  }
])
