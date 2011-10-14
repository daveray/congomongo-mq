;  Copyright (c) Dave Ray, 2011. All rights reserved.

;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this 
;   distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns congomongo-mq.test.core
  (:require [somnium.congomongo :as mongo])
  (:use [congomongo-mq.core])
  (:use [clojure.test]))

(def test-db "congomongo-mq-test")
(def test-db-host "127.0.0.1")

(defn setup! [] (mongo/mongo! :db test-db :host test-db-host))
(defn teardown! [] (mongo/drop-database! test-db))

(defmacro with-test-mongo [& body]
  `(try
     (setup!)
     ~@body
     (finally (teardown!))))

(deftest message-queue 
  (testing "init! creates a capped collection"
    (with-test-mongo
      (init! :test-queue 5)
      (is (mongo/collection-exists? :test-queue))
      (doseq [i (range 10)]
        (enqueue! :test-queue i))
      (is (= 5 (count (mongo/fetch :test-queue))))))

  (testing "dequeue! returns nil on an empty queue"
    (with-test-mongo
      (init! :dequeue 5)
      (is (nil? (dequeue! :dequeue)))))

  (testing "is fifo"
    (with-test-mongo
      (init! :fifo 10)
      (doseq [i (range 10)]
        (enqueue! :fifo i))
      (is (= (range 10) (map content (drain! :fifo)))))))

(deftest garbage-collection
  (testing "gc will return unprocessed messages to the queue"
    (with-test-mongo
      (init! :gc-queue 5)
      (enqueue! :gc-queue "message")
      (is (= "message" (content (dequeue! :gc-queue))))
      (is (= 1 (gc! :gc-queue 0)))
      (is (= "message" (content (dequeue! :gc-queue))))))
  (testing "gc won't touch a done! message"
    (with-test-mongo
      (init! :gc-queue 10)
      (enqueue! :gc-queue "message")
      (done! :gc-queue (dequeue! :gc-queue))
      (gc! :gc-queue 0)
      (is (nil? (dequeue! :gc-queue)))))
  (testing "with-next-message ensures that done! is called"
    (with-test-mongo
      (init! :gc-queue 10)
      (enqueue! :gc-queue  "hello")
      (is (= "HELLO" (with-next-message :gc-queue m
        (.toUpperCase "hello"))))
      (is (= 0 (gc! :gc-queue 0))))))

