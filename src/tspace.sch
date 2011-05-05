;;;
;;;; Simple-minded implementation of Tuple-spaces
;;;
;;
;; @created   "Wed Apr 18 06:32:06 EDT 2007"
;; @author    "Dominique Boucher"
;; 


(define-macro (tspace-put! object)
  (match object
    ((tag . fields)
     `(tspace:put! ,tag (list ,@fields)))
    (_else
     (error "invalid object in tspace-put! - " object))))


(define-macro (tspace-read pattern . body)
  (tspace-retrieve pattern body 'tspace:read))


(define-macro (tspace-get! pattern . body)
  (tspace-retrieve pattern body 'tspace:get!))

