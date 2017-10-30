(ns riemann-generic.core
  (:require [riemann.streams :refer :all]
            [riemann.config :refer :all]
            [riemann.test :refer :all]
            [riemann-cond-dt.core :refer :all]
            [clojure.tools.logging :refer :all]))


(defn threshold
  [{:keys [name params output] :as opts}]
  (where (and (service (:service params))
              ((:operation params) (:threshold params)))
    (with :state "critical"
      output)))

(defn threshold-throttle
  [{:keys [name params output] :as opts}]
  (where (and (service (:service params))
           ((:operation params) (:threshold params)))
    (throttle (:nb-events params) (:duration params)
      (with :state "critical"
        output))))

(defn above-dt
  [{:keys [name params output] :as opts}]
  (above (:threshold params) (:duration params)
    (with :state "critical"
      output)))

(defn below-dt
  [{:keys [name params output] :as opts}]
  (below (:threshold params) (:duration params)
    (with :state "critical"
      output)))

(defn outside-dt
  [{:keys [name params output] :as opts}]
  (outside (:min-threshold params) (:max-threshold params) (:duration params)
    (with :state "critical"
      output)))

(defn between-dt
  [{:keys [name params output] :as opts}]
  (between (:min-threshold params) (:max-threshold params) (:duration params)
    (with :state "critical"
      output)))

(defn critical-dt
  [{:keys [name params output] :as opts}]
  (critical (:duration params)
    (with :state "critical"
      output)))
