# notification-service

# Architecture Diagram

```
client <----api----> server <----pool----> postgreSQL database
                       ^
                       |
                       |
                       |
                       v
                 redis + bull
                       ^
                       |
                       |
                       |
                       v
                  worker pool
                       ^
                       |
                       |
                       |
                       v
                providers (email, sms, push)
```

# Steps

- [x] **1 - simple fire + forget**

  - client and server communicate via APIs
  - dont save to db, just send notification (fire and forget) which means there might be errors but no retry logic

- [x] **2 - PostgreSQL Queue polling**

  - Learning Objective: See why using a database as a queue doesn't scale
  - client and server communicate via APIs
  - save to db
    - add docker for postgres
  - poll db for notifications
  - problem is we're scanning the entire table!

- [x] **3 - redis queue w bull**

  - Learning Objective: Proper message queues are fast, but watch for duplicate sends!
  - using bull & redis, bull has easy to setup dashboard
  - proboem is some duplicate sends as the queue retries, solve w idempotency

- [x] **4 - idempotency & deduplication**
  - Learning Objective: Implement idempotency to prevent duplicate sends
  - add new table for idempotency tracking and just check befroe processing notification
