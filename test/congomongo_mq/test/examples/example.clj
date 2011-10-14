(ns congomongo-mq.test.examples.example
  (:require [somnium.congomongo :as mongo]
            [congomongo-mq.core :as mq]))

(defn worker [id conn]
  (fn [] 
    (Thread/sleep 100)
    (mongo/with-mongo conn
      (mq/with-next-message :tasks {:keys [x y] :as m}
        (mq/enqueue! :results (assoc m :sum (+ x y) :id id))))
    (recur)))

(defn read-results [conn]
  (Thread/sleep 50)
  (mq/with-next-message :results {:keys [id x y sum]}
    (println (format "Result [%d]: %d + %d = %d" id x y sum)))
  (recur conn))

(defn -main [& args]
  (let [c (mongo/make-connection "example")] 
    (mongo/with-mongo c 
      (println "Creating :tasks queue")
      (mq/init! :tasks 500) 
      (println "Creating :results queue")
      (mq/init! :results 500)
      (println "Creating workers")
      (doseq [i (range 10)]
        (future ((worker i c))))
      (println "Queueing tasks")
      (doseq [x (range 10) y (range 10)]
        (mq/enqueue! :tasks {:x x :y y}))
      (println "Reading results")
      (read-results c))))

