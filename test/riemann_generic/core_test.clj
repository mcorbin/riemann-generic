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
  (test-stream (threshold {:service "foo" :warning 30 :critical 70})
               [{:service "foo" :metric 29}
                {:service "foo" :metric 30}
                {:service "foo" :metric 40}
                {:service "foo" :metric 70}
                {:service "bar" :metric 70}
                {:service "foo" :metric 90}]
               [{:service "foo" :metric 30 :state "warning"}
                {:service "foo" :metric 40 :state "warning"}
                {:service "foo" :metric 70 :state "critical"}
                {:service "foo" :metric 90 :state "critical"}]))

(deftest threshold-fn-test
  (test-stream (threshold {:service "foo" :critical 70 :operation >})
               [{:service "foo" :metric 40}
                {:service "foo" :metric 70}
                {:service "bar" :metric 70}
                {:service "foo" :metric 90}]
               [{:service "foo" :metric 70 :state "critical"}
                {:service "foo" :metric 90 :state "critical"}]))

(deftest above-test
  (test-stream (above {:threshold 70 :duration 10})
               [{:metric 40 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}]))


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
  (test-stream (between {:min-threshold 70 :max-threshold 90 :duration 10})
               [{:metric 100 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}]))

(deftest outside-test
  (test-stream (outside {:min-threshold 70 :max-threshold 90 :duration 10})
               [{:metric 80 :time 0}
                {:metric 100 :time 1}
                {:metric 101 :time 12}
                {:metric 1 :time 13}
                {:metric 70 :time 14}]
               [{:metric 101 :state "critical" :time 12}
                {:metric 1 :state "critical" :time 13}]))
