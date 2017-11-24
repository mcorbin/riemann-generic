(ns riemann-generic.core
  (:require [riemann.streams :refer :all]
            [riemann.config :refer :all]
            [riemann.test :refer :all]
            [riemann-cond-dt.core :as dt]
            [clojure.tools.logging :refer :all]))

;; (setq clojure-defun-style-default-indent t)

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
              ((:operation opts) (:threshold opts)))
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn above
  [opts & children]
  (dt/above (:threshold opts) (:duration opts)
   (with :state "critical"
     (fn [event]
       (call-rescue event children)))))

(defn below
  [opts & children]
  (dt/below (:threshold opts) (:duration opts)
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn outside
  [opts & children]
  (dt/outside (:min-threshold opts) (:max-threshold opts) (:duration opts)
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn between
  [opts & children]
  (dt/between (:min-threshold opts) (:max-threshold opts) (:duration opts)
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn critical
  [opts & children]
  (dt/critical (:duration opts)
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))
