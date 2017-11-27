(ns riemann-generic.core
  (:require [riemann.streams :refer :all]
            [riemann.config :refer :all]
            [riemann.test :refer :all]
            [riemann-cond-dt.core :as dt]
            [clojure.tools.logging :refer :all]))

;; (setq clojure-defun-style-default-indent t)

(defn threshold
  "Filter events using the `:service` opts value, compare the `:metric` event
  value with the values of `:warning` in `:critical` in `opts` and update the
  event state accordely, and forward to children.

  `opts` keys:
  - `:service`  : Filter all events using `(service (:service opts))`
  - `:critical` : A number, the event `:state` will be set to `critical` if the
  event metric is >= to the value.
  - `:warning`  : A number, the event `:state` will be set to `warning` if the
  event metric is < to `:critical` and >= to `:warning` (optional)

  Example:

  (threshold {:service \"foo\" :warning 30 :critical 70} email)"
  [opts & children]
  (where (service (:service opts))
    (when (:warning opts)
      (where (and (< (:metric event) (:critical opts))
                  (>= (:metric event) (:warning opts)))
        (with :state "warning"
          (fn [event]
            (call-rescue event children)))))
    (where (>= (:metric event) (:critical opts))
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn threshold-fn
  "Filter events using the `:service` opts value, use the `:warning-fn` and
  `:critical-fn` values (which should be function accepting an event)
  to set the event `:state` accordely. Forward events to children

  `opts` keys:
  - `:service`  : Filter all events using `(service (:service opts))`
  - `:critical-fn` : A function accepting an event and returning a boolean.
  - `:warning-fn`  : A function accepting an event and returning a boolean (optional).

  Example:

  (threshold-fn {:service \"foo\"
                 :warning-fn #(and (>= (:metric %) 30)
                                   (< (:metric %) 70))
                 :critical-fn #(>= (:metric %) 70)})

  In this example, event :state will be \"warning\" if `:metric` is >= 30 and < 70
  and \"critical\" if `:metric` is >= 70"
  [opts & children]
  (where (service (:service opts))
    (when (:warning-fn opts)
      (where ((:warning-fn opts) event)
        (with :state "warning"
          (fn [event]
            (call-rescue event children)))))
    (where ((:critical-fn opts) event)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn threshold-fn-crit
  [opts & children]
  (where (and (service (:service opts))
              ((:operation opts) (:critical opts)))
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn above
  [opts & children]
  (where (service (:service opts))
    (dt/above (:threshold opts) (:duration opts)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn below
  [opts & children]
  (where (service (:service opts))
    (dt/below (:threshold opts) (:duration opts)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn outside
  [opts & children]
  (where (service (:service opts))
    (dt/outside (:min-threshold opts) (:max-threshold opts) (:duration opts)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn between
  [opts & children]
  (where (service (:service opts))
    (dt/between (:min-threshold opts) (:max-threshold opts) (:duration opts)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn critical
  [opts & children]
  (where (service (:service opts))
    (dt/critical (:duration opts)
      (fn [event]
        (call-rescue event children)))))

(defn expired-service
  [opts & children]
  (expired
    (where (service (:service opts))
      (fn [event]
        (call-rescue event children)))))

(defn generate-stream
  [[stream-key config]]
  (let [s (condp = stream-key
            :threshold threshold
            :threshold-fn threshold-fn
            :above above
            :below below
            :outside outside
            :between between
            :critical critical)
        children (:children config)]
    (apply (partial s (dissoc config :children))
           children)))

(defn generate-streams
  [config]
  (let [children (mapv generate-stream config)]
    (fn [event]
      (call-rescue event children))))



;; {:threshold [{:service "foo" :warning 60 :critical 80 :children [email pagerduty]}
;;              {:service "bar" :warning 30 :critical 120 :children [email]}]
;;  :above [{:threshold 90 :duration 60 :service "foo" :children [pagerduty]}
;;          {:threshold 50 :duration 20 :service "bar" :children [email]}]
;;  :threshold-fn [{:service "baz" :operation > :critical 80 :children [email]}
;;                 {:service "baz" :operation < :critical 20 :children [email]}]
;;  :between [{:service "foo"
;;             :min-threshold 60
;;             :max-threshold 80
;;             :duration 80
;;             :children [email]}]}
