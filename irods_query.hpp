
#ifndef IRODS_QUERY_HPP
#define IRODS_QUERY_HPP

#ifdef RODS_SERVER
#include "rsGenQuery.hpp"
#else
#include "genQuery.h"
#endif

#include "rcMisc.h"

char *getCondFromString( char * t );

namespace irods {
    namespace query_helper {
#ifdef RODS_SERVER
        typedef rsComm_t comm_type;
        static const std::function<
            int(rsComm_t*,
                genQueryInp_t*,
                genQueryOut_t**)>
                    query_fcn{rsGenQuery};
#else
        typedef rcComm_t comm_type;
        static const std::function<
            int(rcComm_t*,
                genQueryInp_t*,
                genQueryOut_t**)>
            query_fcn{rcGenQuery};
#endif
    };

    class query {
    public:
        typedef std::vector<std::string> value_type;


        class iterator: public std::iterator<
            std::forward_iterator_tag,// iterator_category
            value_type,               // value_type
            value_type,               // difference_type
            const value_type*,        // pointer
            value_type> {             // reference
                query_helper::comm_type*           comm_;
                const std::string    query_string_;
                const uintmax_t      max_rows_;
                uint32_t             row_idx_;
                genQueryInp_t*       gen_input_; 
                genQueryOut_t*       gen_output_; 
                bool                 end_iteration_state_;

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
                    advance_query();
                    return *this;
                }
                
                iterator operator++(int) {
                    iterator ret = *this;
                    ++(*this);
                    return ret;
                }

                bool operator==(const iterator& _rhs) const {
                    if(end_iteration_state_ && _rhs.end_iteration_state_) {
                        return true;
                    }
                    return (query_string_ == _rhs.query_string_);
                }
               
                bool operator!=(const iterator& _rhs) const {
                    bool val = !(*this == _rhs);
                    return val;
                }

                value_type operator*() {
                    return capture_results();
                }

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

                    const int query_err = query_helper::query_fcn(comm_, gen_input_, &gen_output_);
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

         }; // class iterator

        explicit query(
            query_helper::comm_type*   _comm,
            const std::string& _query_string,
            uintmax_t          _max_rows = MAX_SQL_ROWS) :
            gen_output_{} {

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

            const int query_err = query_helper::query_fcn(
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

        ~query() {
            clearGenQueryInp(&gen_input_);
            freeGenQueryOut(&gen_output_); 
        }

        iterator   begin() {return *iter_;}
        iterator   end()   {return iterator();}
        value_type front() {return (*(*iter_));}

        size_t size()  {
            if(!gen_output_) {return 0;}
            return gen_output_->rowCnt;
        }
    private:
        genQueryInp_t             gen_input_; 
        genQueryOut_t*            gen_output_; 
        std::unique_ptr<iterator> iter_;
    }; // class query
} // namespace irods

#endif // IRODS_QUERY_HPP

