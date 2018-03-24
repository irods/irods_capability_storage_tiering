
#ifndef IRODS_QUERY_HPP
#define IRODS_QUERY_HPP

#ifdef RODS_SERVER
#include "rsGenQuery.hpp"
#include "specificQuery.h"
#include "rsSpecificQuery.hpp"
#else
#include "genQuery.h"
#include "specificQuery.h"
#endif

#include "rcMisc.h"

#include <algorithm>

char *getCondFromString( char * t );

namespace irods {
    namespace query_helper {
#ifdef RODS_SERVER
        typedef rsComm_t comm_type;
        static const std::function<
            int(rsComm_t*,
                genQueryInp_t*,
                genQueryOut_t**)>
                    gen_query_fcn{rsGenQuery};
        static const std::function<
            int(rsComm_t*,
                specificQueryInp_t *specificQueryInp,
                genQueryOut_t **)>
                    spec_query_fcn{rsSpecificQuery};
#else
        typedef rcComm_t comm_type;
        static const std::function<
            int(rcComm_t*,
                genQueryInp_t*,
                genQueryOut_t**)>
            gen_query_fcn{rcGenQuery};
        static const std::function<
            int(rcComm_t*,
                specificQueryInp_t *specificQueryInp,
                genQueryOut_t **)>
                    spec_query_fcn{rcSpecificQuery};
#endif
    };

    class query {
    public:
        typedef std::vector<std::string> value_type;

        enum query_type {
            GENERAL = 0,
            SPECIFIC = 1
        };

        static query_type convert_string_to_query_type(
                const std::string& _str) {
            // default option
            if(_str.empty()) {
                return GENERAL;
            }

            const std::string GEN_STR{"general"};
            const std::string SPEC_STR{"specific"};

            std::string lowered{_str};
            std::transform(
                lowered.begin(),
                lowered.end(),
                lowered.begin(),
                ::tolower);

            if(GEN_STR == lowered) {
                return GENERAL;
            }
            else if(SPEC_STR == lowered) {
                return SPECIFIC;
            }
            else {
                THROW(
                    SYS_INVALID_INPUT_PARAM,
                    _str + " - is not a query type");
            }
        } // convert_string_to_query_type

        class query_impl_base {
            public:

            size_t size() {
                if(!gen_output_) {
                    return 0;
                }
                return gen_output_->rowCnt;
            }

            int cont_idx() {
                return gen_output_->continueInx;
            }

            int row_cnt() {
                return gen_output_->rowCnt;
            }

            std::string query_string() {
                return query_string_;
            }

            bool page_in_flight(const int row_idx_) {
                if(row_idx_ < row_cnt()) {
                    return true;
                }
                return false;
            }

            bool query_complete() {
                // finished page, and out of pages
                if(cont_idx() <= 0) {
                    return true;
                }
                return false;
            }

            value_type capture_results(int _row_idx) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d - row idx %d attriCnt %d", __FUNCTION__, __LINE__, _row_idx, gen_output_->attriCnt);
                value_type res;
                for(int attr_idx = 0; attr_idx < gen_output_->attriCnt; ++attr_idx) {
                    uint32_t offset = gen_output_->sqlResult[attr_idx].len * _row_idx;
                    std::string str{&gen_output_->sqlResult[attr_idx].value[offset]};
rodsLog(LOG_NOTICE, "XXXX - %s:%d - attr idx %d, str [%s]", __FUNCTION__, __LINE__, attr_idx, str.c_str());
                    res.push_back(str);
                }
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                return res;
            }

            bool results_valid() {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                if(gen_output_) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d - row cnt %d", __FUNCTION__, __LINE__, gen_output_->rowCnt);
                    return (gen_output_->rowCnt > 0);
                }
                else {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    return false;
                }
            }

            virtual int fetch_page() = 0;
            virtual ~query_impl_base() {
                freeGenQueryOut(&gen_output_);
            }

            query_impl_base(
                query_helper::comm_type* _comm,
                const std::string&       _q) :
                comm_{_comm},
                query_string_{_q},
                gen_output_{} {
            };
            protected:
            query_helper::comm_type* comm_;
            const std::string& query_string_;
            genQueryOut_t* gen_output_;
        }; // class query_impl_base

        class gen_query_impl : public query_impl_base {
            public:
            virtual ~gen_query_impl() {
            }

            int fetch_page() {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                if(gen_output_) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    freeGenQueryOut(&gen_output_);
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    gen_input_.continueInx = gen_output_->continueInx;
                }

rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                int ret = query_helper::gen_query_fcn(
                           comm_,
                           &gen_input_,
                           &gen_output_);
rodsLog(LOG_NOTICE, "XXXX - %s:%d - row cnt %d", __FUNCTION__, __LINE__, gen_output_->rowCnt);
                return ret;
            } // fetch_page

            gen_query_impl(
                query_helper::comm_type* _comm,
                int                      _max_rows,
                const std::string&       _query_string) :
                query_impl_base(_comm, _query_string) {

rodsLog(LOG_NOTICE, "XXXX - %s:%d - qstr [%s] max rows [%d]", __FUNCTION__, __LINE__, _query_string.c_str(),_max_rows);
                memset(&gen_input_, 0, sizeof(gen_input_));
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                gen_input_.maxRows = _max_rows;
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                const int fill_err = fillGenQueryInpFromStrCond(
                                         const_cast<char*>(_query_string.c_str()),
                                         &gen_input_);
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                if(fill_err < 0) {
                    THROW(
                        fill_err,
                        boost::format("query fill failed for [%s]") %
                        _query_string);
                }
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
            } // ctor

            private:
            genQueryInp_t gen_input_; 
        };


        class iterator: public std::iterator<
            std::forward_iterator_tag,// iterator_category
            value_type,               // value_type
            value_type,               // difference_type
            const value_type*,        // pointer
            value_type> {             // reference
                query_helper::comm_type* comm_;
                const std::string query_string_;
                uintmax_t max_rows_;//const uintmax_t max_rows_;
                uint32_t row_idx_;
                genQueryInp_t* gen_input_; 
                genQueryOut_t* gen_output_; 
                bool end_iteration_state_;

                query_impl_base* query_impl_;

                public:
                explicit iterator () :
                    comm_{},
                    query_string_{},
                    max_rows_{},
                    row_idx_{},
                    gen_input_{},
                    gen_output_{},
                    end_iteration_state_{true} {
                }

                explicit iterator(
                    query_impl_base* _qimp) :
                    query_impl_(_qimp),
                    row_idx_{},
                    end_iteration_state_{false} {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                }
                explicit iterator(
                    query_helper::comm_type* _comm,
                    const std::string&       _query_string,
                    const uintmax_t&         _max_rows,
                    genQueryInp_t*           _gen_input,
                    genQueryOut_t*           _gen_output) :
                    comm_{_comm},
                    query_string_{_query_string},
                    max_rows_{_max_rows},
                    row_idx_{},
                    gen_input_{_gen_input},
                    gen_output_{_gen_output},
                    end_iteration_state_{false} {
                } // ctor

                iterator operator++() {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    advance_query();
                    return *this;
                }
                
                iterator operator++(int) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    iterator ret = *this;
                    ++(*this);
                    return ret;
                }

                bool operator==(const iterator& _rhs) const {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    if(end_iteration_state_ && _rhs.end_iteration_state_) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                        return true;
                    }
rodsLog(LOG_NOTICE, "XXXX - %s:%d ql [%s] qr [%s]", __FUNCTION__, __LINE__, query_impl_->query_string().c_str(), _rhs.query_string_.c_str());
                    //return (query_string_ == _rhs.query_string_);
                    return (query_impl_->query_string() == _rhs.query_string_);
                }
               
                bool operator!=(const iterator& _rhs) const {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    bool val = !(*this == _rhs);
                    return val;
                }

                value_type operator*() {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    return capture_results();
                }
#if 0
                void advance_query() {
                    row_idx_++;

                    // not at end of page, have valid results
                    if(row_idx_ < gen_output_->rowCnt) {
                        return;
                    }

                    // finished page, and out of pages
                    if(gen_output_->continueInx <= 0) {
                        end_iteration_state_ = true;
                        return;
                    }

                    // finished page, attempt to fetch another
                    row_idx_ = 0; // reset row counter
                    gen_input_->continueInx = gen_output_->continueInx;
                    freeGenQueryOut(&gen_output_);

                    const int query_err = query_helper::gen_query_fcn(comm_, gen_input_, &gen_output_);
                    if(query_err < 0) {
                        if(CAT_NO_ROWS_FOUND != query_err) {
                            THROW(
                                query_err,
                                boost::format("gen query failed for [%s] on idx %d") %
                                query_string_ %
                                gen_input_->continueInx);
                        }

                       end_iteration_state_ = true;
                
                    } // if

                } // advance_query

                value_type capture_results() {
                    value_type res;
                    for(int attr_idx = 0; attr_idx < gen_output_->attriCnt; ++attr_idx) {
                        uint32_t offset = gen_output_->sqlResult[attr_idx].len * row_idx_;
                        std::string str{&gen_output_->sqlResult[attr_idx].value[offset]};
                        res.push_back(str);
                    }
                    return res;
                }
#else
                void advance_query() {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    row_idx_++;

rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    if(query_impl_->page_in_flight(row_idx_)) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                        return;
                    }

rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    if(query_impl_->query_complete()) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                        end_iteration_state_ = true;
                        return;
                    }

rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    const int query_err = query_impl_->fetch_page();
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    if(query_err < 0) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                        if(CAT_NO_ROWS_FOUND != query_err) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                            THROW(
                                query_err,
                                boost::format("gen query failed for [%s] on idx %d") %
                                query_string_ %
                                gen_input_->continueInx);
                        }

                       end_iteration_state_ = true;
                
                    } // if
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);

                } // advance_query 

                value_type capture_results() {
rodsLog(LOG_NOTICE, "XXXX - %s:%d - row_irx_ %d", __FUNCTION__, __LINE__, row_idx_);
                    return query_impl_->capture_results(row_idx_);
                }
#endif
         }; // class iterator
#if 0
        explicit query(
            query_helper::comm_type*   _comm,
            const std::string& _query_string,
            uintmax_t _max_rows = MAX_SQL_ROWS,
            query_type _query_type = GENERAL) :
            gen_output_{},
            query_type_{_query_type} {

            memset(&gen_input_, 0, sizeof(gen_input_));
            gen_input_.maxRows = _max_rows;
            const int fill_err = fillGenQueryInpFromStrCond(
                                     const_cast<char*>(_query_string.c_str()),
                                     &gen_input_);
            if(fill_err < 0) {
                THROW(
                    fill_err,
                    boost::format("query fill failed for [%s]") %
                    _query_string);
            }

            const int query_err = query_helper::gen_query_fcn(
                                      _comm,
                                      &gen_input_,
                                      &gen_output_);
            if(query_err < 0) {
                if(CAT_NO_ROWS_FOUND == query_err) {
                    iter_ = std::make_unique<iterator>();
                }
                else {
                    THROW(
                        query_err,
                        boost::format("gen query failed for [%s]") %
                        _query_string);
                }
            }

            if(gen_output_ && (gen_output_->rowCnt > 0)) {
                iter_ = std::make_unique<iterator>(_comm, _query_string, _max_rows, &gen_input_, gen_output_);
            }
            else {
                iter_ = std::make_unique<iterator>();
            }
        } // query ctor
#else
        explicit query(
            query_helper::comm_type* _comm,
            const std::string&       _query_string,
            uintmax_t                _max_rows   = MAX_SQL_ROWS,
            query_type               _query_type = GENERAL) :
            query_type_{_query_type} {
rodsLog(LOG_NOTICE, "XXXX - %s:%d - qstr [%s] max rows [%d]", __FUNCTION__, __LINE__, _query_string.c_str(),_max_rows);
                if(query_type_ == GENERAL) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    query_impl_ = new gen_query_impl(
                                          _comm,
                                          _max_rows,
                                          _query_string);
                }

rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                const int fetch_err = query_impl_->fetch_page(); 
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                if(fetch_err < 0) {
                    if(CAT_NO_ROWS_FOUND == fetch_err) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                        iter_ = std::make_unique<iterator>();
                    }
                    else {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                        THROW(
                            fetch_err,
                            boost::format("query failed for [%s] type [%d]") %
                            _query_string %
                            _query_type);
                    }
                }

rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                if(query_impl_->results_valid()) {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    iter_ = std::make_unique<iterator>(query_impl_);
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                }
                else {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
                    iter_ = std::make_unique<iterator>();
                }
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
        } // ctor
#endif
        ~query() {
            //clearGenQueryInp(&gen_input_);
            //freeGenQueryOut(&gen_output_); 
        }

        iterator   begin() {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
            return *iter_;
        }
        iterator   end()   {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
            return iterator();
        }
        value_type front() {
rodsLog(LOG_NOTICE, "XXXX - %s:%d", __FUNCTION__, __LINE__);
            return (*(*iter_));
        }

        size_t size()  {
#if 0
            if(!gen_output_) {return 0;}
            return gen_output_->rowCnt;
#else
            return query_impl_->size();
#endif
        }
    private:
        genQueryInp_t             gen_input_; 
        genQueryOut_t*            gen_output_; 
        const query_type          query_type_;
        std::unique_ptr<iterator> iter_;

        query_impl_base*          query_impl_;
    }; // class query
} // namespace irods

#endif // IRODS_QUERY_HPP

