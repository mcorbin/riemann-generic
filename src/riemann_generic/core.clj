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

(defn above
  "Filter events using the `:service` opts value.

  If the condition `(> (:metric event) threshold)` is valid for all events
  received during at least the period `dt`, valid events received after the `dt`
  period will be passed on until an invalid event arrives. Forward to children.
  `:metric` should not be nil (it will produce exceptions).

  `opts` keys:
  - `:service`   : Filter all events using `(service (:service opts))`
  - `:threshold` : The threshold used by the above stream
  - `:duration`   : The time period in seconds.

  Example:

  (above {:threshold 70 :duration 10 :service \"bar\"} email)

  Set `:state` to \"critical\" if events `:metric` is > to 70 during 10 sec or more."
  [opts & children]
  (where (service (:service opts))
    (dt/above (:threshold opts) (:duration opts)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn below
  "Filter events using the `:service` opts value.

  If the condition `(< (:metric event) threshold)` is valid for all events
  received during at least the period `dt`, valid events received after the `dt`
  period will be passed on until an invalid event arrives. Forward to children.
  `:metric` should not be nil (it will produce exceptions).

  `opts` keys:
  - `:service`   : Filter all events using `(service (:service opts))`
  - `:threshold` : The threshold used by the above stream
  - `:duration`   : The time period in seconds.

  Example:

  (below {:threshold 70 :duration 10 :service \"bar\"} email)

  Set `:state` to \"critical\" if events `:metric` is < to 70 during 10 sec or more."
  [opts & children]
  (where (service (:service opts))
    (dt/below (:threshold opts) (:duration opts)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn outside
  "Filter events using the `:service` opts value.

  If the condition `(or (< (:metric event) low) (> (:metric event) high))` is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives.

  `opts` keys:
  - `:service`   : Filter all events using `(service (:service opts))`
  - `:min-threshold` : The min threshold
  - `:max-threshold` : The max threshold
  - `:duration`   : The time period in seconds.

  Example:

  (outside {:min-threshold 70
            :max-threshold 90
            :duration 10
            :service \"bar\"})

  Set `:state` to \"critical\" if events `:metric` is < to 70 or > 90 during 10 sec or more."
  [opts & children]
  (where (service (:service opts))
    (dt/outside (:min-threshold opts) (:max-threshold opts) (:duration opts)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn between
  "Filter events using the `:service` opts value.

  If the condition `(and (> (:metric event) low) (< (:metric event) high))` is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives.

  `:metric` should not be nil (it will produce exceptions).
  `opts` keys:
  - `:service`   : Filter all events using `(service (:service opts))`
  - `:min-threshold` : The min threshold
  - `:max-threshold` : The max threshold
  - `:duration`   : The time period in seconds.

  Example:

  (between {:min-threshold 70
            :max-threshold 90
            :duration 10
            :service \"bar\"})

  Set `:state` to \"critical\" if events `:metric` is > to 70 and < 90 during 10 sec or more."
  [opts & children]
  (where (service (:service opts))
    (dt/between (:min-threshold opts) (:max-threshold opts) (:duration opts)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn critical
  "Filter events using the `:service` opts value.

  Takes a time period in seconds `dt`.
  If all events received during at least the period `dt` have `:state` critical, new critical events received after the `dt` period will be passed on until an invalid event arrives.

  `opts` keys:
  - `:service`   : Filter all events using `(service (:service opts))`
  - `:duration`   : The time period in seconds.

  Example:

  (critical {:service \"bar\" :duration \"10\"} email)

  Set `:state` to \"critical\" if events `:state` is critical during 10 sec or more."
  [opts & children]
  (where (service (:service opts))
    (dt/critical (:duration opts)
      (fn [event]
        (call-rescue event children)))))

(defn percentile-crit
  [opts & children]
  (where (service (str (:service opts) " " (:point opts)))
    (when-let [warning-fn (:warning-fn opts)]
      (where (warning-fn event)
        (with :state "warning"
          (fn [event]
            (call-rescue event children)))))
    (where ((:critical-fn opts) event)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn percentiles-crit
  "Calculates percentiles and alert on it.

  `opts` keys:
  - `:service`   : Filter all events using `(service (:service opts))`
  - `:duration`  : The time period in seconds.
  - `:points`    : A map, the keys are the percentiles points.
  The value should be a map with these keys:
  - `:critical-fn` a function accepting an event and returning a boolean.
  - `:warning-fn` a function accepting an event and returning a boolean.
  For each point, if the event match `:warning-fn` and `:critical-fn`, the event `:state` will be \"warning\" or \"critical\"

Example:

(percentiles-crit {:service \"api req\"
                   :duration 20
                   :points {1 {:critical-fn #(> (:metric %) 100)
                               :warning-fn #(> (:metric %) 100)}
                            0.50 {:critical-fn #(> (:metric %) 500)}
                            0 {:critical-fn #(> (:metric %) 1000)}}}"
  [opts & children]
  (let [points (mapv first (:points opts))
        percentiles-streams (mapv (fn [[point conf]]
                                    (percentile-crit
                                     (assoc conf :service (:service opts)
                                                 :point point)
                                      (first children)))
                              (:points opts))
        children (conj percentiles-streams (second children))]
    (where (service (:service opts))
      (percentiles (:duration opts) points
        (fn [event]
          (call-rescue event children))))))

(defn scount
  [opts & children]
  (where (service (:service opts))
    (fixed-time-window 20
      (smap riemann.folds/count
        (fn [event]
          (call-rescue (assoc event :service (str (:service opts) " count"))
            children))))))

(defn scount-crit
  [opts & children]
  (scount opts
    (where ((:critical-fn opts) event)
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))
    (when-let [warning-fn (:warning-fn opts)]
      (where (warning-fn event)
        (with :state "warning"
          (fn [event]
            (call-rescue event children)))))))

(defn generate-stream
  [[stream-key config]]
  (let [s (condp = stream-key
            :threshold threshold
            :threshold-fn threshold-fn
            :above above
            :below below
            :outside outside
            :percentiles-crit percentiles-crit
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
