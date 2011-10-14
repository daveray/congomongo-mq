;  Copyright (c) Dave Ray, 2011. All rights reserved.

;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this 
;   distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns congomongo-mq.core
  (:require [somnium.congomongo :as mongo]))

; TODO I always feel dirty using java.util.Date. Is there a better option?
; Should this be configurable?
(defn- timestamp [offset-millis]
  (if (= 0 offset-millis)
    (java.util.Date.)
    (+ (.. (java.util.Date.) getTime) offset-millis)))

(defn init!
  "Initialize a queue in collection named by queue and max size max.
  
  Must be called inside (with-mongo).

  Examples:
  
    (init! :my-queue 10000)
  "
  [queue max]
  (mongo/create-collection! queue :capped true :max max))

(defn enqueue! 
  "Enqueue some content
  
  Must be called inside (with-mongo).

  Examples:
    
    (enqueue! :my-queue {:x 10 :y 99})
  "
  [queue content]
  (mongo/insert! queue {:in-prog false 
                        :done    false 
                        :start   (timestamp 0) 
                        :content content} ))

(defn dequeue!
  "Dequeue the next message. Access the content with (congomongo-mq.core/content).
  You must call (congomongo-mq.core/done!) when you're done with the message.
  Better yet, use (congomongo-mq.core/with-next-message*).
  
  Returns nil if no messages are available.

  Must be called inside (with-mongo).

  Examples:

    (dequeue! :my-queue)
    ;=> nil
    (enqueue! :my-queue {:x 10 :y 99})
    (def m (dequeue! :my-queue))
    ;=> A message
    (content m)
    ;=> {:x 10 :y 99}
    (done! m) 
  "
  [queue]
  (mongo/fetch-and-modify queue 
                          {:in-prog false :done false} 
                          {:$set { :in-prog true :start (timestamp 0) } } :sort {:_id 1}))

(defn drain! 
  "Dequeue messages until nil is returned and return them."
  [queue]
  (loop [acc [] next (dequeue! queue)]
    (if next
      (recur (conj acc next) (dequeue! queue))
      acc)))

(defn content 
  "Retrieve the content of a message dequeued with (congomongo-mq.core/dequeue!)."
  [message] 
  (:content message))

(defn done!
  "Finish processing a message retrieved with (congomongo-mq.core/dequeue!)
  Consider using (congomongo-mq.core/with-next-message) instead.
  
  Must be called inside (with-mongo).
  "
  [queue message]
  (mongo/update! queue 
                 {:_id (:_id message)} 
                 {:$set { :done true :in-prog false } }
                 :upsert false))

(defn with-next-message* 
  "Dequeue a message, call (f (content message) & args), and call (congomongo-mq.core/done!).
  Returns the result of f, or nil if no message was available.
  
  Must be called inside (with-mongo).

  See:
    (congomongo-mq.core/with-next-message)
  "
  [queue f & args]
  (when-let [message (dequeue! queue)]
    (let [result (apply f (content message) args)]
      (done! queue message)
      result)))

(defmacro with-next-message
  "Dequeue a message, bind it to v and evalute body, returning the result
  of the body. Calls (congomongo-mq.core/done!) automatically.

  Must be called inside (with-mongo).
  
  Examples:
  
    ; Suppose next message is {:first-name \"Foo\" :last-name \"Bar\" }
    (with-next-message {:keys [first-name last-name]}
      (str last-name \", \" first-name))
    ;=> \"Bar, Foo\"

  See:
    (congomongo-mq.core/with-next-message*)
  "
  [queue v & body]
  `(with-next-message*
     ~queue
     (fn [~v]
       ~@body)))

(defn gc!
  "Run garbage collection on the queue, that is reset any messages that have taken too
  long to finish processing. ms is milliseconds expected to process a message.
  
  Returns the number of reset messages."
  [queue ms]
  (.getN (mongo/update! queue 
                  {:in-prog true :start {:$lte (timestamp (- ms))}} 
                  {:$set {:in-prog false}}
                  :multiple true
                  :upsert false)))

