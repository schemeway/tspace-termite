;;;
;;;; Simple-minded implementation of Tuple-spaces
;;;
;;
;; @created   "Wed Apr 18 06:32:06 EDT 2007"
;; @author    "Dominique Boucher"
;; 


(declare
 (standard-bindings)
 (block)
 (not safe)
 (fixnum))

(define-structure waiting-process label predicate pid tag remove?)
(define-structure space tag process)


(define-structure object-repository objects)

(define (create-object-repository)
  (make-object-repository '()))

(define (add-object-to-repository repository object)
  (object-repository-objects-set! 
   repository
   (cons object (object-repository-objects repository))))

(define (remove-object-from-repository repository object)
  (object-repository-objects-set!
   repository
   (remove (lambda (obj) (eq? object obj))
	   (object-repository-objects repository))))

(define (search-repository repository matches?)
  (find matches? (object-repository-objects repository)))


(define (space-loop)

  (define waiting-queue '())
  (define tuple-space   (make-dict))
  (define repository    (create-object-repository)) 

  (define (add-tuple object)
    (add-object-to-repository repository object))

  (define (put-tuple object)
    (let loop ((waiting-processes waiting-queue))
      (if (null? waiting-processes)
          (add-tuple object)
          (let ((process (car waiting-processes)))
            (if ((waiting-process-predicate process) object)
                (begin
                  (set! waiting-queue (remove (lambda (proc) (eq? proc process)) waiting-queue))
                  (send-to-process process object)
                  (if (waiting-process-remove? process)
                      #f
                      (loop (cdr waiting-processes))))
                (loop (cdr waiting-processes)))))))
  
  
  (define (read-tuple process)
    (let ((object (search-repository repository (waiting-process-predicate process))))
      (if object
	  (begin
	    (send-to-process process object)
	    (if (waiting-process-remove? process)
		(remove-object-from-repository repository object)))
	  (add-waiting-process process))))
  
  
  (define (add-waiting-process process)
    (set! waiting-queue (cons process waiting-queue)))


  (define (send-to-process process object)
    (! (waiting-process-pid process) (list (waiting-process-tag process) object)))


  (let loop ()
    (recv
     (('put object)
      (put-tuple object)
      (loop))
     (('read waiting-process)
      (read-tuple waiting-process)
      (loop))
     (('stop)
      'done))))


(define (tspace-server-loop)
  
  (define spaces (make-dict))
  
  (define (match-pattern patterns)
    (lambda (object)
      (let loop ((patterns patterns)
                 (object  object))
        (cond ((null? patterns)
               (null? object))
              (else
               (let ((first-pattern (car patterns)))
                 (cond ((eq? first-pattern '_)
                        (and (pair? object)
                             (loop (cdr patterns) (cdr object))))
                       ((pair? object)
                        (and (equal? (car object) first-pattern)
                             (loop (cdr patterns) (cdr object))))
                       (else
                        #f))))))))

  
  (define (find-space label)
    (let ((space (dict-ref spaces label)))
      (or space
	  (let ((new-space (make-space label (spawn space-loop))))
	    (dict-set! spaces label new-space)
	    new-space))))

  (define (send-to-space label message)
    (let ((space (find-space label)))
      (! (space-process space) message)))


  (let loop ()
    (recv 
     ((pid tag ('put label object))
      (send-to-space label (list 'put object))
      (loop))
     ((pid tag ('read label pattern))
      (send-to-space label (list 'read (make-waiting-process label (match-pattern pattern) pid tag #f)))
      (loop))
     ((pid tag ('get label pattern))
      (send-to-space label (list 'read (make-waiting-process label (match-pattern pattern) pid tag #t)))
      (loop))
     (('stop)
      'done))))


;;;
;;;; List utilities (we should use SRFI-1 instead...)
;;;

(define (remove  pred l) (filter  (lambda (x) (not (pred x))) l))

(define (find pred list)
  (cond ((find-tail pred list) => car)
	(else #f)))

(define (find-tail pred list)
  (let lp ((list list))
    (and (not (null-list? list))
         (if (pred (car list)) list
             (lp (cdr list))))))

(define (null-list? l)
  (cond ((pair? l) #f)
	((null? l) #t)
	(else (error "null-list?: argument out of domain" l))))

(define (iota count #!optional (start 0) (step 1))
  (if (< count 0) (error "Negative step count" iota count))
  (let ((last-val (+ start (* (- count 1) step))))
    (do ((count count (- count 1))
         (val last-val (- val step))
         (ans '() (cons val ans)))
        ((<= count 0)  ans))))

;;;
;;;; High-level client API
;;;


(define (tspace-start)
  (publish-service 'tuple-space (spawn tspace-server-loop)))


(define (tspace-stop)
  (! (resolve-service 'tuple-space) '(stop)))


(define (tspace-connect node)
  (publish-service 'tuple-space (on node (lambda () (resolve-service 'tuple-space)))))


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


;;;
;;;; Low-level client (private) API
;;;


(define (tspace-retrieve pattern body-list retriever-name)
  (match pattern
    ((tag . fields)
     (where (list? fields))
     (let ((variables (filter pair?
                              (map (lambda (object index)
                                     (if (and (symbol? object) (not (eq? '_ object)))
                                         (cons object index)
                                         #f))
                                   fields
                                   (iota (length fields)))))
           (fields    (map (lambda (object)
                             (if (symbol? object)
                                 '(quote _)
                                 object))
                           fields))
           (tuple-var (gensym)))
       `(let ((,tuple-var (,retriever-name ,tag (list ,@fields))))
          (let ,(map (lambda (var/index)
                      `(,(car var/index) (list-ref ,tuple-var ,(cdr var/index))))
                    variables)
            ,@body-list))))
    (_else
     (error "invalid tspace pattern: " pattern))))


(define (tspace:put! tag fields)
  (! (resolve-service 'tuple-space) (list (self) (make-tag) `(put ,tag ,fields))))


(define (tspace:read tag template)
  (!? (resolve-service 'tuple-space) `(read ,tag ,template)))


(define (tspace:get! tag template)
  (!? (resolve-service 'tuple-space) `(get ,tag ,template)))


