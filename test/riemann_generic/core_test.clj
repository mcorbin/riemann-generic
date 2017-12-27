(ns riemann-generic.core-test
  (:require [clojure.test :refer :all]
            [riemann.time.controlled :refer :all]
            [riemann.time :refer :all]
            [riemann.test :refer [run-stream run-stream-intervals test-stream
                                  with-test-stream test-stream-intervals]]
            [riemann-generic.core :refer :all]))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(deftest threshold-test
  (testing "with warning key"
    (test-stream (threshold {:warning 30 :critical 70})
      [{:metric 29}
       {:metric 30}
       {:metric 40}
       {:metric 70}
       {:metric 90}]
      [{:metric 30 :state "warning"}
       {:metric 40 :state "warning"}
       {:metric 70 :state "critical"}
       {:metric 90 :state "critical"}]))
  (testing "without warning key"
    (test-stream (threshold {:critical 70})
      [{:metric 29}
       {:metric 30}
       {:metric 40}
       {:metric 70}
       {:metric 90}]
      [{:metric 70 :state "critical"}
       {:metric 90 :state "critical"}])))

(deftest threshold-fn-test
  (testing "critical"
    (test-stream (threshold-fn {:critical-fn #(>= (:metric %) 70)})
      [{:metric 40}
       {:metric 70}
       {:metric 90}]
      [{:metric 70 :state "critical"}
       {:metric 90 :state "critical"}]))
  (testing "warning and critical"
    (test-stream (threshold-fn {:warning-fn #(and (>= (:metric %) 30)
                                               (< (:metric %) 70))
                                :critical-fn #(>= (:metric %) 70)})
      [{:metric 29}
       {:metric 30}
       {:metric 40}
       {:metric 70}
       {:metric 90}]
      [{:metric 30 :state "warning"}
       {:metric 40 :state "warning"}
       {:metric 70 :state "critical"}
       {:metric 90 :state "critical"}])))

(deftest above-test
  (test-stream (above {:threshold 70 :duration 10})
               [{:metric 40 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}])
  (test-stream (above {:threshold 70 :duration 10})
               [{:metric 80 :time 1}
                {:metric 40 :time 12}
                {:metric 90 :time 13}]
               []))

(deftest threshold-during-fn-test
  (test-stream (threshold-during-fn {:threshold-fn #(and 
                                           (> (:metric %) 42) 
                                           (compare (:service %) "foo"))
                                      :duration 10
                                      :state "critical"})
               [{:metric 40 :time 0  :service "foo"}
                {:metric 43 :time 1  :service "foo"}
                {:metric 43 :time 2  :service "bar"}
                {:metric 44 :time 12 :service "foo"}
                {:metric 45 :time 13 :service "foo"}
                {:metric 10 :time 14 :service "foo"}
                {:metric 66 :time 15 :service "bar"}]
               [{:metric 44 :state "critical" :time 12 :service "foo"}
                {:metric 45 :state "critical" :time 13 :service "foo"}])
    (test-stream (threshold-during-fn {:threshold-fn #(and 
                                           (> (:metric %) 42) 
                                           (compare (:service %) "foo"))
                                       :duration 10
                                       :state "disaster"})
               [{:metric 40 :time 0  :service "foo"}
                {:metric 43 :time 1  :service "foo"}
                {:metric 43 :time 2  :service "bar"}
                {:metric 44 :time 12 :service "foo"}
                {:metric 45 :time 13 :service "foo"}
                {:metric 10 :time 14 :service "foo"}
                {:metric 66 :time 15 :service "bar"}]
               [{:metric 44 :state "disaster" :time 12 :service "foo"}
                {:metric 45 :state "disaster" :time 13 :service "foo"}])
    (test-stream (threshold-during-fn {:threshold-fn #(and 
                                           (> (:metric %) 42) 
                                           (compare (:service %) "foo"))
                                       :duration 10
                                       :state "critical"})
               [{:metric 40 :time 0  :service "foo"}
                {:metric 43 :time 1  :service "foo"}
                {:metric 43 :time 2  :service "bar"}
                {:metric 12 :time 6  :service "foo"}
                {:metric 44 :time 12 :service "foo"}
                {:metric 45 :time 13 :service "foo"}
                {:metric 10 :time 14 :service "foo"}
                {:metric 66 :time 15 :service "bar"}]
               []))


(deftest below-test
  (test-stream (below {:threshold 70 :duration 10})
               [{:metric 80 :time 0}
                {:metric 40 :time 1}
                {:metric 40 :time 12}
                {:metric 41 :time 13}
                {:metric 90 :time 14}]
               [{:metric 40 :state "critical" :time 12}
                {:metric 41 :state "critical" :time 13}]))

(deftest between-test
  (test-stream (between {:min-threshold 70
                         :max-threshold 90
                         :duration 10})
               [{:metric 100 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}]))

(deftest outside-test
  (test-stream (outside {:min-threshold 70
                         :max-threshold 90
                         :duration 10})
               [{:metric 80 :time 0}
                {:metric 100 :time 1}
                {:metric 101 :time 12}
                {:metric 1 :time 13}
                {:metric 70 :time 14}]
               [{:metric 101 :state "critical" :time 12}
                {:metric 1 :state "critical" :time 13}]))

(deftest critical-test
  (test-stream (critical {:duration 10})
               [{:time 0 :state "critical"}
                {:time 1 :state "critical"}
                {:time 12 :state "critical"}
                {:time 13 :state "critical"}
                {:time 14 :state "ok"}]
               [{:state "critical" :time 12}
                {:state "critical" :time 13}]))

(deftest generate-streams-test
  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:duration 10
                                         :children [child]}]})]
    (s {:time 0 :service "baz" :state "critical"})
    (s {:time 1 :service "bar" :state "critical"})
    (s {:time 12 :service "bar" :state "critical"})
    (s {:time 13 :service "lol" :state "critical"})
    (s {:time 14  :service "bar" :state "ok"})
    (is (= @out [{:state "critical" :time 12 :service "bar"}
                 {:state "critical" :time 13 :service "lol"}])))
  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:where #(= (:service %) "bar")
                                         :duration 10
                                         :children [child]}]})]
    (s {:time 0 :service "bar" :state "critical"})
    (s {:time 1 :service "bar" :state "critical"})
    (s {:time 12 :service "bar" :state "critical"})
    (s {:time 13 :service "bar" :state "critical"})
    (s {:time 14  :service "bar" :state "ok"})
    (is (= @out [{:state "critical" :time 12 :service "bar"}
                 {:state "critical" :time 13 :service "bar"}])))
  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:where #(= (:service %) "bar")
                                         :duration 10
                                         :children [child]}]
                             :threshold [{:where #(= (:service %) "foo")
                                          :warning 30
                                          :critical 70
                                          :children [child]}]})]
    (s {:time 0 :service "bar" :state "critical"})
    (s {:time 1 :service "bar" :state "critical"})
    (s {:time 12 :service "bar" :state "critical"})
    (s {:time 13 :service "bar" :state "critical"})
    (s {:time 14  :service "bar" :state "ok"})
    (s {:service "foo" :metric 29})
    (s {:service "foo" :metric 30})
    (s {:service "foo" :metric 40})
    (s {:service "foo" :metric 70})
    (s {:service "bar" :metric 70})
    (s {:service "foo" :metric 90})
    (is (= @out [{:state "critical" :time 12 :service "bar"}
                 {:state "critical" :time 13 :service "bar"}
                 {:service "foo" :metric 30 :state "warning"}
                 {:service "foo" :metric 40 :state "warning"}
                 {:service "foo" :metric 70 :state "critical"}
                 {:service "foo" :metric 90 :state "critical"}]))))

(deftest percentile-crit-test
  (test-stream (percentile-crit {:service "api req"
                                 :critical-fn #(> (:metric %) 100)
                                 :point 0.99})
    [{:time 0 :metric 110 :service "api req 0.99"}
     {:time 0 :metric 110 :service "api req 0.95"}
     {:time 0 :metric 90 :service "api req 0.99"}]
    [{:time 0 :metric 110 :state "critical" :service "api req 0.99"}]))

(deftest percentiles-crit-test
  (let [out1 (atom [])
        child1 #(swap! out1 conj %)
        out2 (atom [])
        child2 #(swap! out2 conj %)
        s (percentiles-crit {:service "api req"
                             :duration 20
                             :points {1 {:critical-fn #(> (:metric %) 100)
                                         :warning-fn #(> (:metric %) 100)}
                                      0.50 {:critical-fn #(> (:metric %) 500)}
                                      0 {:critical-fn #(> (:metric %) 1000)
                                         :critical 1000}}}
            child1
            child2)]
    (s {:time 0 :service "api req" :metric 0})
    (s {:time 0 :service "api req" :metric 100})
    (s {:time 1 :service "api req" :metric 200})
    (advance! 22)
    (s {:time 23 :service "api req" :metric 30})
    (s {:time 23 :service "api req" :metric 40})
    (s {:time 23 :service "api req" :metric 501})
    (s {:time 23 :service "api req" :metric 503})
    (s {:time 24 :service "api req" :metric 600})
    (s {:time 24 :service "api req" :metric 600})
    (s {:time 25 :service "api req" :metric 601})
    (advance! 41)
    (is (= @out1
          [{:time 1 :service "api req 1" :metric 200 :state "warning"}
           {:time 1 :service "api req 1" :metric 200 :state "critical"}
           {:time 25 :service "api req 1" :metric 601 :state "warning"}
           {:time 25 :service "api req 1" :metric 601 :state "critical"}
           {:time 23 :service "api req 0.5" :metric 503 :state "critical"}]))
    (is (= @out2
          [{:time 1 :service "api req 1" :metric 200}
           {:time 0 :service "api req 0.5" :metric 100}
           {:time 0 :service "api req 0" :metric 0}
           {:time 25 :service "api req 1" :metric 601}
           {:time 23 :service "api req 0.5" :metric 503}
           {:time 23 :service "api req 0" :metric 30}]))))

(deftest scount-test
  (test-stream (scount {:duration 20})
    [{:time 1}
     {:time 19}
     {:time 30}
     {:time 31}
     {:time 35}
     {:time 61}]
    [{:time 1 :metric 2}
     {:time 30 :metric 3}
     ;; no event during the time window
     (riemann.common/event {:metric 0 :time 61})]))

(deftest scount-crit-test
  (test-stream (scount-crit {:service "foo"
                             :duration 20
                             :critical-fn #(> (:metric %) 5)})
    [{:time 1}
     {:time 2}
     {:time 3}
     {:time 4}
     {:time 5}
     {:time 19}
     {:time 30}
     {:time 31}
     {:time 35}
     {:time 61}]
    [{:time 1 :metric 6 :state "critical"}]))


(def kafka-output #(println % " => event"))
