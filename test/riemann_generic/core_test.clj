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
  (testing "with warning key and signature"
    (test-stream (threshold {:warning 30 :critical 70 :signature-fn #(str (:host %) "_ggwp!")})
      [{:metric 29 :host "foo"}
       {:metric 30 :host "foo"}
       {:metric 40 :host "foo"}
       {:metric 70 :host "foo"}
       {:metric 90 :host "foo"}]
      [{:metric 30 :host "foo" :state "warning"  :signature "foo_ggwp!"}
       {:metric 40 :host "foo" :state "warning"  :signature "foo_ggwp!"}
       {:metric 70 :host "foo" :state "critical" :signature "foo_ggwp!"}
       {:metric 90 :host "foo" :state "critical" :signature "foo_ggwp!"}]))
  (testing "without warning key"
    (test-stream (threshold {:critical 70 :signature-fn #(str (:host %) "_ggwp!")})
      [{:metric 29 :host "foo"}
       {:metric 30 :host "foo"}
       {:metric 40 :host "foo"}
       {:metric 70 :host "foo"}
       {:metric 90 :host "foo"}]
      [{:metric 70 :host "foo" :state "critical" :signature "foo_ggwp!"}
       {:metric 90 :host "foo" :state "critical" :signature "foo_ggwp!"}])))


(deftest threshold-fn-test
  (testing "critical with signature"
    (test-stream (threshold-fn {:critical-fn #(>= (:metric %) 70) :signature-fn #(str (:host %) "_ggwp!")})
      [{:metric 40 :host "foo"}
       {:metric 70 :host "foo"}
       {:metric 90 :host "foo"}]
      [{:metric 70 :host "foo" :state "critical" :signature "foo_ggwp!"}
       {:metric 90 :host "foo" :state "critical" :signature "foo_ggwp!"}]))
  (testing "warning and critical with signature"
    (test-stream (threshold-fn {:warning-fn #(and (>= (:metric %) 30)
                                               (< (:metric %) 70))
                                :critical-fn #(>= (:metric %) 70)
                                :signature-fn #(str (:host %) "_hfgl!")})
      [{:metric 29 :host "foo"}
       {:metric 30 :host "foo"}
       {:metric 40 :host "foo"}
       {:metric 70 :host "foo"}
       {:metric 90 :host "foo"}]
      [{:metric 30 :host "foo" :state "warning" :signature "foo_hfgl!"}
       {:metric 40 :host "foo" :state "warning" :signature "foo_hfgl!"}
       {:metric 70 :host "foo" :state "critical":signature "foo_hfgl!"}
       {:metric 90 :host "foo" :state "critical":signature "foo_hfgl!"}])))

(deftest above-test
  (testing "should not alert"
    (test-stream (above {:threshold 70 :duration 10 :signature-fn #(str (:metric %) "_a70_d10")})
               [{:metric 80 :time 1}
                {:metric 40 :time 12}
                {:metric 90 :time 13}]
               []))
  (testing "should alert only if > 70 during 10 with signature"
    (test-stream (above {:threshold 70 :duration 10 :signature-fn #(str (:metric %) "_a70_d10")})
               [{:metric 40 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12 :signature "80_a70_d10"}
                {:metric 81 :state "critical" :time 13 :signature "81_a70_d10"}])))

(deftest below-test
  (testing "should alert if < 70 during 10s with signature"
    (test-stream (below {:threshold 70 :duration 10 :signature-fn #(str (:metric %) "_<70_d10")})
               [{:metric 80 :time 0}
                {:metric 40 :time 1}
                {:metric 40 :time 12}
                {:metric 41 :time 13}
                {:metric 90 :time 14}]
               [{:metric 40 :state "critical" :time 12 :signature "40_<70_d10"}
                {:metric 41 :state "critical" :time 13 :signature "41_<70_d10"}]))
  (testing "should not alert"
    (test-stream (below {:threshold 70 :duration 10 :signature-fn #(str (:metric %) "_<70_d10")})
               [{:metric 80 :time 0}
                {:metric 71 :time 1}
                {:metric 42 :time 6}
                {:metric 81 :time 12}
                {:metric 42 :time 13}
                {:metric 91 :time 14}]
               [])))

(deftest threshold-during-fn-test
  (testing "should alert if metric >42 and service is foo"
    (test-stream (threshold-during-fn {:threshold-fn #(and 
                                           (> (:metric %) 42) 
                                           (compare (:service %) "foo"))
                                       :duration 10
                                       :state "critical"
                                       :signature-fn #(str (:metric %) "_ggwp!")})
               [{:metric 40 :time 0  :service "foo"}
                {:metric 43 :time 1  :service "foo"}
                {:metric 43 :time 2  :service "bar"}
                {:metric 44 :time 12 :service "foo"}
                {:metric 45 :time 13 :service "foo"}
                {:metric 10 :time 14 :service "foo"}
                {:metric 66 :time 15 :service "bar"}]
               [{:metric 44 :state "critical" :time 12 :service "foo" :signature "44_ggwp!"}
                {:metric 45 :state "critical" :time 13 :service "foo" :signature "45_ggwp!"}]))
  (testing "should alert if metric >42 and service is foo"
    (test-stream (threshold-during-fn {:threshold-fn #(and 
                                           (< (:metric %) 42) 
                                           (compare (:service %) "foo"))
                                       :duration 10
                                       :state "disaster"
                                       :signature-fn #(str (:metric %) "_ggwp!")})
               [{:metric 42 :time 0  :service "foo"}
                {:metric 41 :time 1  :service "foo"}
                {:metric 41 :time 2  :service "bar"}
                {:metric 40 :time 12 :service "foo"}
                {:metric 41 :time 13 :service "foo"}
                {:metric 66 :time 14 :service "foo"}
                {:metric 41 :time 15 :service "foo"}
                {:metric 12 :time 15 :service "bar"}
                {:metric 66 :time 16 :service "foo"}]
               [{:metric 40 :state "disaster" :time 12 :service "foo" :signature "40_ggwp!"}
                {:metric 41 :state "disaster" :time 13 :service "foo" :signature "41_ggwp!"}]))
  (testing "should not alert"
    (test-stream (threshold-during-fn {:threshold-fn #(and 
                                           (< (:metric %) 42) 
                                           (compare (:service %) "bar"))
                                       :duration 10
                                       :state "critical"
                                       :signature-fn #(str (:metric %) "_ggwp!")})
               [{:metric 40 :time 0  :service "foo"}
                {:metric 43 :time 1  :service "foo"}
                {:metric 43 :time 2  :service "bar"}
                {:metric 12 :time 6  :service "foo"}
                {:metric 44 :time 12 :service "foo"}
                {:metric 45 :time 13 :service "foo"}
                {:metric 10 :time 14 :service "foo"}
                {:metric 66 :time 15 :service "bar"}]
               [])))

(deftest between-test
  (testing "should alert if > 70 and <90 during 10s with signature"
    (test-stream (between {:min-threshold 70
                         :max-threshold 90
                         :duration 10
                         :signature-fn  #( str (:metric %) "_>70_<90_d10")})
               [{:metric 100 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12 :signature "80_>70_<90_d10"}
                {:metric 81 :state "critical" :time 13 :signature "81_>70_<90_d10"}])))

(deftest outside-test
  (testing "should alert if < 70 and >90 during 10s with signature"
    (test-stream (outside {:min-threshold 70
                         :max-threshold 90
                         :duration 10
                         :signature-fn #( str (:metric %) "_<70_>90_d10")})
               [{:metric 80 :time 0}
                {:metric 100 :time 1}
                {:metric 101 :time 12}
                {:metric 1 :time 13}
                {:metric 70 :time 14}]
               [{:metric 101 :state "critical" :time 12 :signature "101_<70_>90_d10"}
                {:metric 1 :state "critical" :time 13 :signature "1_<70_>90_d10"}])))

(deftest critical-test
  (test-stream (critical {:duration 10 :signature-fn (fn [n] (str "c_d10"))})
               [{:time 0 :state "critical"}
                {:time 1 :state "critical"}
                {:time 12 :state "critical"}
                {:time 13 :state "critical"}
                {:time 14 :state "ok"}]
               [{:state "critical" :time 12 :signature "c_d10"}
                {:state "critical" :time 13 :signature "c_d10"}]))

(deftest percentile-crit-test
  (test-stream (percentile-crit {:service "api req"
                                 :critical-fn #(> (:metric %) 100)
                                 :point 0.99
                                 :signature-fn #(str (:service %) "_pc_p0.99")})
    [{:time 0 :metric 110 :service "api req 0.99"}
     {:time 0 :metric 110 :service "api req 0.95"}
     {:time 0 :metric 90 :service "api req 0.99"}]
    [{:time 0 :metric 110 :state "critical" :service "api req 0.99" :signature "api req 0.99_pc_p0.99" }]))

(deftest percentiles-crit-test
  (let [out1 (atom [])
        child1 #(swap! out1 conj %)
        out2 (atom [])
        child2 #(swap! out2 conj %)
        s (percentiles-crit {:service "api req"
                             :duration 20
                             :signature-fn #(str (:metric %) "_percentiles-crit-test")
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
          [{:time 1 :service "api req 1"    :metric 200 :state "warning"  :signature "200_percentiles-crit-test"}
           {:time 1 :service "api req 1"    :metric 200 :state "critical" :signature "200_percentiles-crit-test"}
           {:time 25 :service "api req 1"   :metric 601 :state "warning"  :signature "601_percentiles-crit-test"}
           {:time 25 :service "api req 1"   :metric 601 :state "critical" :signature "601_percentiles-crit-test"}
           {:time 23 :service "api req 0.5" :metric 503 :state "critical" :signature "503_percentiles-crit-test"}]))
    (is (= @out2
          [{:time 1 :service "api req 1"    :metric 200 :signature "200_percentiles-crit-test"}
           {:time 0 :service "api req 0.5"  :metric 100 :signature "100_percentiles-crit-test"}
           {:time 0 :service "api req 0"    :metric 0   :signature "0_percentiles-crit-test"}
           {:time 25 :service "api req 1"   :metric 601 :signature "601_percentiles-crit-test"}
           {:time 23 :service "api req 0.5" :metric 503 :signature "503_percentiles-crit-test"}
           {:time 23 :service "api req 0"   :metric 30  :signature "30_percentiles-crit-test"}]))))

(deftest scount-test
  (test-stream (scount {:duration 20 :signature-fn (fn [n] (str "count"))})
    [{:time 1}
     {:time 19}
     {:time 30}
     {:time 31}
     {:time 35}
     {:time 61}]
    [{:time 1 :metric 2 :signature "count" }
     {:time 30 :metric 3 :signature "count" }
     ;; no event during the time window
     (riemann.common/event {:metric 0 :time 61 :signature "count"})]))

(deftest scount-crit-test
  (test-stream (scount-crit {:service "foo"
                             :duration 20
                             :critical-fn #(> (:metric %) 5)
                             :signature-fn (fn [n] (str "sc>5_d20"))})
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
    [{:time 1 :metric 6 :state "critical" :signature "sc>5_d20"}]))

(deftest generate-streams-test
  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:duration 10
                                         :children [child]
                                         :signature-fn #(str (:service %) "_c_d10!")}]})]
    (s {:time 0 :service "baz" :state "critical"})
    (s {:time 1 :service "bar" :state "critical"})
    (s {:time 12 :service "bar" :state "critical"})
    (s {:time 13 :service "lol" :state "critical"})
    (s {:time 14  :service "bar" :state "ok"})
    (is (= @out [{:state "critical" :time 12 :service "bar" :signature "bar_c_d10!"}
                 {:state "critical" :time 13 :service "lol" :signature "lol_c_d10!"}])))
  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:where #(= (:service %) "bar")
                                         :duration 10
                                         :children [child]
                                         :signature-fn #(str (:service %) "_ggwp!")}]})]
    (s {:time 0 :service "bar" :state "critical"})
    (s {:time 1 :service "bar" :state "critical"})
    (s {:time 12 :service "bar" :state "critical"})
    (s {:time 13 :service "bar" :state "critical"})
    (s {:time 14  :service "bar" :state "ok"})
    (is (= @out [{:state "critical" :time 12 :service "bar" :signature "bar_ggwp!"}
                 {:state "critical" :time 13 :service "bar" :signature "bar_ggwp!"}])))
  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:where #(= (:service %) "bar")
                                         :duration 10
                                         :children [child]
                                         :signature-fn #(str (:service %) "_hfgl!")}]
                             :threshold [{:where #(= (:service %) "foo")
                                          :warning 30
                                          :critical 70
                                          :signature-fn #(str (:service %) "_ggwp!")
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
    (is (= @out [{:state "critical" :time 12 :service "bar" :signature "bar_hfgl!"}
                 {:state "critical" :time 13 :service "bar" :signature "bar_hfgl!"}
                 {:service "foo" :metric 30 :state "warning"  :signature "foo_ggwp!"}
                 {:service "foo" :metric 40 :state "warning"  :signature "foo_ggwp!"}
                 {:service "foo" :metric 70 :state "critical" :signature "foo_ggwp!"}
                 {:service "foo" :metric 90 :state "critical" :signature "foo_ggwp!"}]))))

;Expiration not testable cf https://github.com/riemann/riemann/issues/415
;(deftest expired-host-test
;  (testing "should alert if expired"
;    (test-stream (expired-host {:ttl 100 :throttle 200 :state "critical" :signature "expired_ttl100_th200"})
;      [{:time 0   :metric 1 :service "foo" :host "a"}
;       {:time 99  :metric 2 :service "foo" :host "a"}
;       {:time 100 :metric 3 :service "foo" :host "a"}
;       {:time 302 :metric 4 :service "foo" :host "a"}
;       {:time 303 :metric 5 :service "foo" :host "a"}]
;      [{:time 302 :state "critical" :service "host up" :host "a" :signature "expired_ttl100_th200"}])))
