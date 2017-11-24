(ns riemann-generic.core
  (:require [riemann.streams :refer :all]
            [riemann.config :refer :all]
            [riemann.test :refer :all]
            [riemann-cond-dt.core :as dt]
            [clojure.tools.logging :refer :all]))

;; (setq clojure-defun-style-default-indent t)


{:threshold [{:service "foo" :warning 60 :critical 80 :children [#(:metric)]}]
 :above [{:threshold 90 :duration 60}]
 }

(defn threshold
  [opts & children]
  (where (service (:service opts))
    (where (and (< (:metric event) (:critical opts))
                (>= (:metric event) (:warning opts)))
      (with :state "warning"
        (fn [event]
          (call-rescue event children))))
    (where (>= (:metric event) (:critical opts))
      (with :state "critical"
        (fn [event]
          (call-rescue event children))))))

(defn threshold-fn
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
