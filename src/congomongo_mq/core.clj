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
  "Initialize a queue in collection named by queue and max size max."
  [queue max]
  (mongo/create-collection! queue :capped true :max max))

(defn enqueue! 
  "Enqueue some content"
  [queue content]
  (mongo/insert! queue {:in-prog false 
                        :done    false 
                        :start   (timestamp 0) 
                        :content content} ))

(defn dequeue!
  "Dequeue the next message. Access the content with (congomongo-mq.core/content).
  You must call (congomongo-mq.core/done!) when you're done with the message.
  Better yet, use (congomongo-mq.core/with-next-message*).
  
  Returns nil if no messages are available."
  [queue]
  (mongo/fetch-and-modify queue 
                          {:in-prog false :done false} 
                          {:$set { :in-prog true :start (timestamp 0) } } :sort {:_id 1}))

(defn content 
  "Retrieve the content of a message dequeued with (congomongo-mq.core/dequeue!)."
  [message] 
  (:content message))

(defn done!
  "Finish processing a message retrieved with (congomongo-mq.core/dequeue!)"
  [queue message]
  (mongo/update! queue 
                 {:_id (:_id message)} 
                 {:$set { :done true :in-prog false } }
                 :upsert false))

(defn with-next-message* 
  "Dequeue a message, call (f (content message) & args), and call (congomongo-mq.core/done!).
  Returns the result of f, or nil if no message was available."
  [queue f & args]
  (when-let [message (dequeue! queue)]
    (let [result (apply f (content message) args)]
      (done! queue message)
      result)))

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

