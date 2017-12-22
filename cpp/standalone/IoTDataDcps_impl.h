#ifndef IOTDATADCPS_IMPL_H_
#define IOTDATADCPS_IMPL_H_

#include "ccpp.h"
#include "ccpp_IoTData.h"
#include "TypeSupportMetaHolder.h"
#include "TypeSupport.h"
#include "FooDataWriter_impl.h"
#include "FooDataReader_impl.h"
#include "FooDataReaderView_impl.h"


namespace DDS {

    namespace IoT {

        class  IoTDataTypeSupportMetaHolder : public ::DDS::OpenSplice::TypeSupportMetaHolder
        {
        public:
            IoTDataTypeSupportMetaHolder ();
            virtual ~IoTDataTypeSupportMetaHolder ();
        
        private:
            ::DDS::OpenSplice::DataWriter * create_datawriter ();
        
            ::DDS::OpenSplice::DataReader * create_datareader ();
        
            ::DDS::OpenSplice::DataReaderView * create_view ();
        };
        
        class  IoTDataTypeSupport : public virtual IoTDataTypeSupportInterface,
                                                           public ::DDS::OpenSplice::TypeSupport
        {
        public:
            IoTDataTypeSupport ();
        
            virtual ~IoTDataTypeSupport ();
        
        private:
            IoTDataTypeSupport (const IoTDataTypeSupport &);
        
            IoTDataTypeSupport & operator= (const IoTDataTypeSupport &);
        };
        
        typedef IoTDataTypeSupportInterface_var IoTDataTypeSupport_var;
        typedef IoTDataTypeSupportInterface_ptr IoTDataTypeSupport_ptr;
        
        class  IoTDataDataWriter_impl : public virtual IoTDataDataWriter,
                                                               public ::DDS::OpenSplice::FooDataWriter_impl
        {
            friend class DDS::OpenSplice::Publisher;
            friend class IoTDataTypeSupportMetaHolder;
        
        public:
            virtual ::DDS::InstanceHandle_t register_instance (
                const IoTData & instance_data) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::InstanceHandle_t register_instance_w_timestamp (
                const IoTData & instance_data,
                const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t unregister_instance (
                const IoTData & instance_data,
                ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t unregister_instance_w_timestamp (
                const IoTData & instance_data,
                ::DDS::InstanceHandle_t handle,
                const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t write (
                const IoTData & instance_data,
                ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t write_w_timestamp (
                const IoTData & instance_data,
                ::DDS::InstanceHandle_t handle,
                const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t dispose (
                const IoTData & instance_data,
                ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t dispose_w_timestamp (
                const IoTData & instance_data,
                ::DDS::InstanceHandle_t handle,
                const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t writedispose (
                const IoTData & instance_data,
                ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t writedispose_w_timestamp (
                const IoTData & instance_data,
                ::DDS::InstanceHandle_t handle,
                const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t get_key_value (
                IoTData & key_holder,
                ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::InstanceHandle_t lookup_instance (
                const IoTData & instance_data) THROW_ORB_EXCEPTIONS;
        
        protected:
            IoTDataDataWriter_impl ();
        
            virtual ~IoTDataDataWriter_impl ();
        
            virtual DDS::ReturnCode_t init (
                            DDS::OpenSplice::Publisher *publisher,
                            DDS::OpenSplice::DomainParticipant *participant,
                            const DDS::DataWriterQos &qos,
                            DDS::OpenSplice::Topic *a_topic,
                            const char *name,
                            DDS::OpenSplice::cxxCopyIn copyIn,
                            DDS::OpenSplice::cxxCopyOut copyOut);
        
        private:
            IoTDataDataWriter_impl (const IoTDataDataWriter_impl &);
        
            IoTDataDataWriter_impl & operator= (const IoTDataDataWriter_impl &);
        };
        
        class  IoTDataDataReader_impl : public virtual IoTDataDataReader,
                                                               public ::DDS::OpenSplice::FooDataReader_impl
        {
            friend class DDS::OpenSplice::Subscriber;
            friend class IoTDataTypeSupportMetaHolder;
            friend class IoTDataDataReaderView_impl;
        
        public:
            virtual ::DDS::ReturnCode_t read (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_w_condition (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_w_condition (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_next_sample (
                IoTData & received_data,
                ::DDS::SampleInfo & sample_info) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_next_sample (
                IoTData & received_data,
                ::DDS::SampleInfo & sample_info) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_instance (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_instance (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_next_instance (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_next_instance (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_next_instance_w_condition (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_next_instance_w_condition (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t return_loan (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t get_key_value (
                IoTData & key_holder,
                ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::InstanceHandle_t lookup_instance (
                const IoTData & instance) THROW_ORB_EXCEPTIONS;
        
        protected:
            IoTDataDataReader_impl ();
        
            virtual ~IoTDataDataReader_impl ();
        
            DDS::ReturnCode_t init (
                    DDS::OpenSplice::Subscriber *subscriber,
                    const DDS::DataReaderQos &qos,
                    DDS::OpenSplice::TopicDescription *a_topic,
                    const char *name,
                    DDS::OpenSplice::cxxCopyIn copyIn,
                    DDS::OpenSplice::cxxCopyOut copyOut);
        
            static void* dataSeqAlloc (
                void * data_values,
                DDS::ULong len);
        
            static void dataSeqLength (
                void * data_values,
                DDS::ULong len);
        
            static void * dataSeqGetBuffer (
                void * data_values,
                DDS::ULong index);
        
            static void dataSeqCopyOut (
                const void * from,
                void * received_data);
            static void copyDataOut(const void *from, void *to);
        
        private:
            IoTDataDataReader_impl (const IoTDataDataReader_impl &);
            IoTDataDataReader_impl & operator= (const IoTDataDataReader_impl &);
        
            static ::DDS::ReturnCode_t check_preconditions (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples);
        };
        
        class  IoTDataDataReaderView_impl : public virtual IoTDataDataReaderView,
                                                                   public ::DDS::OpenSplice::FooDataReaderView_impl
        {
            friend class DDS::OpenSplice::DataReader;
            friend class IoTDataTypeSupportMetaHolder;
        
        public:
            virtual ::DDS::ReturnCode_t read (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_w_condition (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_w_condition (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_next_sample (
                IoTData & received_data,
                ::DDS::SampleInfo & sample_info) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_next_sample (
                IoTData & received_data,
                ::DDS::SampleInfo & sample_info) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_instance (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_instance (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_next_instance (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_next_instance (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::SampleStateMask sample_states,
                ::DDS::ViewStateMask view_states,
                ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t read_next_instance_w_condition (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t take_next_instance_w_condition (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq,
                ::DDS::Long max_samples,
                ::DDS::InstanceHandle_t a_handle,
                ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t return_loan (
                IoTDataSeq & received_data,
                ::DDS::SampleInfoSeq & info_seq) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::ReturnCode_t get_key_value (
                IoTData & key_holder,
                ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS;
        
            virtual ::DDS::InstanceHandle_t lookup_instance (
                const IoTData & instance) THROW_ORB_EXCEPTIONS;
        
        protected:
            IoTDataDataReaderView_impl ();
        
            virtual ~IoTDataDataReaderView_impl ();
        
            virtual DDS::ReturnCode_t init (
                DDS::OpenSplice::DataReader *reader,
                const char *name,
                const DDS::DataReaderViewQos &qos,
                DDS::OpenSplice::cxxCopyIn copyIn,
                DDS::OpenSplice::cxxCopyOut copyOut);
        
        private:
            IoTDataDataReaderView_impl (const IoTDataDataReaderView_impl &);
        
            IoTDataDataReaderView_impl & operator= (const IoTDataDataReaderView_impl &);
        };
        
    }

}

#undef OS_API
#endif
