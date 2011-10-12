# congomongo-mq

Implement a message queue in MongoDB with Clojure using [CongoMongo] (https://github.com/aboekhoff/congomongo). Inspired by [this post] (http://blog.boxedice.com/2011/09/28/replacing-rabbitmq-with-mongodb).

# Usage
A REPL session:

    user=> (require '[somnium.congomongo :as mongo])
    user=> (use 'congomongo-mq.core)
    user=> (require '[congomongo-mq.core :as mq])
    user=> (mongo/mongo! :db :q-test) ;; <- In real life use make-connection, etc.

    ; Initialize the queue with a max size
    user=> (mq/init! :queue 10)

    ; Enqueue some messages
    user=> (mq/enqueue! :queue "first")
    user=> (mq/enqueue! :queue "second")

    ; Dequeue a message. Make sure to call (done!) when you're done with it.
    user=> (mq/dequeue! :queue)
    user=> (content *1)
    "first"
    user=> (mq/done! :queue *2)

    ; Calling (done!) is a hassle...
    user=> (mq/with-next-message* :queue #(format "Got message '%s'" %))
    "Got message 'second'"

    ; (dequeue!) returns nil when there's nothing left in the queue
    user=> (mq/dequeue! :queue)
    nil

## License

Copyright (C) Dave Ray, 2011

Distributed under the Eclipse Public License, the same as Clojure.
