#include "IoTDataDcps_impl.h"
#include "IoTDataSplDcps.h"
//

extern v_copyin_result
__DDS_IoT_IoTData__copyIn (
    c_base base,
    const struct DDS::IoT::IoTData *from,
    struct _DDS_IoT_IoTData *to);

extern void
__DDS_IoT_IoTData__copyOut (
    const void *_from,
    void *_to);

// DDS DDS::IoT::IoTData TypeSupportMetaHolder Object Body
DDS::IoT::IoTDataTypeSupportMetaHolder::IoTDataTypeSupportMetaHolder () :
        DDS::OpenSplice::TypeSupportMetaHolder ("DDS::IoT::IoTData", "", "typeName,instanceId")
{
    copyIn = (DDS::OpenSplice::cxxCopyIn) __DDS_IoT_IoTData__copyIn;
    copyOut = (DDS::OpenSplice::cxxCopyOut) __DDS_IoT_IoTData__copyOut;
    metaDescriptorArrLength = 48;
    metaDescriptorLength = 5573;
    const char *tmp[] = {"<MetaData version=\"1.0.0\"><Module name=\"DDS\"><Module name=\"IoT\"><Enum name=\"IoTType\"><Element name=\"TYPE_IoTUI8\" value=\"0\"/>",
"<Element name=\"TYPE_IoTUI16\" value=\"1\"/><Element name=\"TYPE_IoTUI32\" value=\"2\"/><Element name=\"TYPE_IoTUI64\" value=\"3\"/>",
"<Element name=\"TYPE_IoTI8\" value=\"4\"/><Element name=\"TYPE_IoTI16\" value=\"5\"/><Element name=\"TYPE_IoTI32\" value=\"6\"/>",
"<Element name=\"TYPE_IoTI64\" value=\"7\"/><Element name=\"TYPE_IoTF32\" value=\"8\"/><Element name=\"TYPE_IoTF64\" value=\"9\"/>",
"<Element name=\"TYPE_IoTB\" value=\"10\"/><Element name=\"TYPE_IoTStr\" value=\"11\"/><Element name=\"TYPE_IoTCh\" value=\"12\"/>",
"<Element name=\"TYPE_IoTUI8Seq\" value=\"13\"/><Element name=\"TYPE_IoTUI16Seq\" value=\"14\"/><Element name=\"TYPE_IoTUI32Seq\" value=\"15\"/>",
"<Element name=\"TYPE_IoTUI64Seq\" value=\"16\"/><Element name=\"TYPE_IoTI8Seq\" value=\"17\"/><Element name=\"TYPE_IoTI16Seq\" value=\"18\"/>",
"<Element name=\"TYPE_IoTI32Seq\" value=\"19\"/><Element name=\"TYPE_IoTI64Seq\" value=\"20\"/><Element name=\"TYPE_IoTF32Seq\" value=\"21\"/>",
"<Element name=\"TYPE_IoTF64Seq\" value=\"22\"/><Element name=\"TYPE_IoTBSeq\" value=\"23\"/><Element name=\"TYPE_IoTStrSeq\" value=\"24\"/>",
"<Element name=\"TYPE_IoTChSeq\" value=\"25\"/></Enum><TypeDef name=\"IoTUI8\"><Octet/></TypeDef><TypeDef name=\"IoTUI16\">",
"<UShort/></TypeDef><TypeDef name=\"IoTUI32\"><ULong/></TypeDef><TypeDef name=\"IoTUI64\"><ULongLong/>",
"</TypeDef><TypeDef name=\"IoTI8\"><Char/></TypeDef><TypeDef name=\"IoTI16\"><Short/></TypeDef><TypeDef name=\"IoTI32\">",
"<Long/></TypeDef><TypeDef name=\"IoTI64\"><LongLong/></TypeDef><TypeDef name=\"IoTF32\"><Float/></TypeDef>",
"<TypeDef name=\"IoTF64\"><Double/></TypeDef><TypeDef name=\"IoTB\"><Boolean/></TypeDef><TypeDef name=\"IoTStr\">",
"<String/></TypeDef><TypeDef name=\"IoTCh\"><Char/></TypeDef><TypeDef name=\"IoTUI8Seq\"><Sequence><Type name=\"IoTUI8\"/>",
"</Sequence></TypeDef><TypeDef name=\"IoTUI16Seq\"><Sequence><Type name=\"IoTUI16\"/></Sequence></TypeDef>",
"<TypeDef name=\"IoTUI32Seq\"><Sequence><Type name=\"IoTUI32\"/></Sequence></TypeDef><TypeDef name=\"IoTUI64Seq\">",
"<Sequence><Type name=\"IoTUI64\"/></Sequence></TypeDef><TypeDef name=\"IoTI8Seq\"><Sequence><Type name=\"IoTI8\"/>",
"</Sequence></TypeDef><TypeDef name=\"IoTI16Seq\"><Sequence><Type name=\"IoTI16\"/></Sequence></TypeDef>",
"<TypeDef name=\"IoTI32Seq\"><Sequence><Type name=\"IoTI32\"/></Sequence></TypeDef><TypeDef name=\"IoTI64Seq\">",
"<Sequence><Type name=\"IoTI64\"/></Sequence></TypeDef><TypeDef name=\"IoTF32Seq\"><Sequence><Type name=\"IoTF32\"/>",
"</Sequence></TypeDef><TypeDef name=\"IoTF64Seq\"><Sequence><Type name=\"IoTF64\"/></Sequence></TypeDef>",
"<TypeDef name=\"IoTBSeq\"><Sequence><Type name=\"IoTB\"/></Sequence></TypeDef><TypeDef name=\"IoTStrSeq\">",
"<Sequence><Type name=\"IoTStr\"/></Sequence></TypeDef><TypeDef name=\"IoTChSeq\"><Sequence><Type name=\"IoTCh\"/>",
"</Sequence></TypeDef><Union name=\"IoTValue\"><SwitchType><Type name=\"IoTType\"/></SwitchType><Case name=\"ui8\">",
"<Type name=\"IoTUI8\"/><Label value=\"TYPE_IoTUI8\"/></Case><Case name=\"ui16\"><Type name=\"IoTUI16\"/>",
"<Label value=\"TYPE_IoTUI16\"/></Case><Case name=\"ui32\"><Type name=\"IoTUI32\"/><Label value=\"TYPE_IoTUI32\"/>",
"</Case><Case name=\"ui64\"><Type name=\"IoTUI64\"/><Label value=\"TYPE_IoTUI64\"/></Case><Case name=\"i8\">",
"<Type name=\"IoTI8\"/><Label value=\"TYPE_IoTI8\"/></Case><Case name=\"i16\"><Type name=\"IoTI16\"/><Label value=\"TYPE_IoTI16\"/>",
"</Case><Case name=\"i32\"><Type name=\"IoTI32\"/><Label value=\"TYPE_IoTI32\"/></Case><Case name=\"i64\">",
"<Type name=\"IoTI64\"/><Label value=\"TYPE_IoTI64\"/></Case><Case name=\"f32\"><Type name=\"IoTF32\"/>",
"<Label value=\"TYPE_IoTF32\"/></Case><Case name=\"f64\"><Type name=\"IoTF64\"/><Label value=\"TYPE_IoTF64\"/>",
"</Case><Case name=\"b\"><Type name=\"IoTB\"/><Label value=\"TYPE_IoTB\"/></Case><Case name=\"str\"><Type name=\"IoTStr\"/>",
"<Label value=\"TYPE_IoTStr\"/></Case><Case name=\"ch\"><Type name=\"IoTCh\"/><Label value=\"TYPE_IoTCh\"/>",
"</Case><Case name=\"ui8Seq\"><Type name=\"IoTUI8Seq\"/><Label value=\"TYPE_IoTUI8Seq\"/></Case><Case name=\"ui16Seq\">",
"<Type name=\"IoTUI16Seq\"/><Label value=\"TYPE_IoTUI16Seq\"/></Case><Case name=\"ui32Seq\"><Type name=\"IoTUI32Seq\"/>",
"<Label value=\"TYPE_IoTUI32Seq\"/></Case><Case name=\"ui64Seq\"><Type name=\"IoTUI64Seq\"/><Label value=\"TYPE_IoTUI64Seq\"/>",
"</Case><Case name=\"i8Seq\"><Type name=\"IoTI8Seq\"/><Label value=\"TYPE_IoTI8Seq\"/></Case><Case name=\"i16Seq\">",
"<Type name=\"IoTI16Seq\"/><Label value=\"TYPE_IoTI16Seq\"/></Case><Case name=\"i32Seq\"><Type name=\"IoTI32Seq\"/>",
"<Label value=\"TYPE_IoTI32Seq\"/></Case><Case name=\"i64Seq\"><Type name=\"IoTI64Seq\"/><Label value=\"TYPE_IoTI64Seq\"/>",
"</Case><Case name=\"f32Seq\"><Type name=\"IoTF32Seq\"/><Label value=\"TYPE_IoTF32Seq\"/></Case><Case name=\"f64Seq\">",
"<Type name=\"IoTF64Seq\"/><Label value=\"TYPE_IoTF64Seq\"/></Case><Case name=\"bSeq\"><Type name=\"IoTBSeq\"/>",
"<Label value=\"TYPE_IoTBSeq\"/></Case><Case name=\"strSeq\"><Type name=\"IoTStrSeq\"/><Label value=\"TYPE_IoTStrSeq\"/>",
"</Case><Case name=\"chSeq\"><Type name=\"IoTChSeq\"/><Label value=\"TYPE_IoTChSeq\"/></Case></Union><Struct name=\"IoTNVP\">",
"<Member name=\"name\"><String/></Member><Member name=\"value\"><Type name=\"IoTValue\"/></Member></Struct>",
"<TypeDef name=\"IoTNVPSeq\"><Sequence><Type name=\"IoTNVP\"/></Sequence></TypeDef><Struct name=\"IoTData\">",
"<Member name=\"typeName\"><String/></Member><Member name=\"instanceId\"><String/></Member><Member name=\"values\">",
"<Type name=\"IoTNVPSeq\"/></Member></Struct></Module></Module></MetaData>"};
    metaDescriptor = new const char*[metaDescriptorArrLength];
    memcpy(metaDescriptor, tmp, sizeof(tmp));
}

DDS::IoT::IoTDataTypeSupportMetaHolder::~IoTDataTypeSupportMetaHolder ()
{
    // Rely on parent destructor.
}

::DDS::OpenSplice::DataWriter *
DDS::IoT::IoTDataTypeSupportMetaHolder::create_datawriter ()
{
    return new DDS::IoT::IoTDataDataWriter_impl();
}

::DDS::OpenSplice::DataReader *
DDS::IoT::IoTDataTypeSupportMetaHolder::create_datareader ()
{
    return new DDS::IoT::IoTDataDataReader_impl();
}

::DDS::OpenSplice::DataReaderView *
DDS::IoT::IoTDataTypeSupportMetaHolder::create_view ()
{
    return new DDS::IoT::IoTDataDataReaderView_impl();
}

// DDS DDS::IoT::IoTData TypeSupport Object Body
DDS::IoT::IoTDataTypeSupport::IoTDataTypeSupport () :
        DDS::OpenSplice::TypeSupport()
{
    tsMetaHolder = new DDS::IoT::IoTDataTypeSupportMetaHolder();
}

DDS::IoT::IoTDataTypeSupport::~IoTDataTypeSupport ()
{
    DDS::release(tsMetaHolder);
}

// DDS DDS::IoT::IoTData DataWriter_impl Object Body
DDS::IoT::IoTDataDataWriter_impl::IoTDataDataWriter_impl ()
{
    // Parent constructor takes care of everything.
}

DDS::IoT::IoTDataDataWriter_impl::~IoTDataDataWriter_impl ()
{
    // Parent destructor takes care of everything.
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::init (
        DDS::OpenSplice::Publisher *publisher,
        DDS::OpenSplice::DomainParticipant *participant,
        const DDS::DataWriterQos &qos,
        DDS::OpenSplice::Topic *a_topic,
        const char *name,
        DDS::OpenSplice::cxxCopyIn copyIn,
        DDS::OpenSplice::cxxCopyOut copyOut)
{
    return DDS::OpenSplice::FooDataWriter_impl::nlReq_init(publisher, participant, qos, a_topic, name, copyIn, copyOut);
}

::DDS::InstanceHandle_t
DDS::IoT::IoTDataDataWriter_impl::register_instance (
    const DDS::IoT::IoTData & instance_data) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::register_instance(&instance_data);
}

::DDS::InstanceHandle_t
DDS::IoT::IoTDataDataWriter_impl::register_instance_w_timestamp (
    const IoTData & instance_data,
    const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::register_instance_w_timestamp(&instance_data, source_timestamp);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::unregister_instance (
    const DDS::IoT::IoTData & instance_data,
    ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::unregister_instance(&instance_data, handle);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::unregister_instance_w_timestamp (
    const IoTData & instance_data,
    ::DDS::InstanceHandle_t handle,
    const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::unregister_instance_w_timestamp(&instance_data, handle, source_timestamp);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::write (
    const DDS::IoT::IoTData & instance_data,
    ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::write(&instance_data, handle);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::write_w_timestamp (
    const IoTData & instance_data,
    ::DDS::InstanceHandle_t handle,
    const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::write_w_timestamp(&instance_data, handle, source_timestamp);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::dispose (
    const DDS::IoT::IoTData & instance_data,
    ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::dispose(&instance_data, handle);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::dispose_w_timestamp (
    const IoTData & instance_data,
    ::DDS::InstanceHandle_t handle,
    const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::dispose_w_timestamp(&instance_data, handle, source_timestamp);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::writedispose (
    const DDS::IoT::IoTData & instance_data,
    ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::writedispose(&instance_data, handle);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::writedispose_w_timestamp (
    const IoTData & instance_data,
    ::DDS::InstanceHandle_t handle,
    const ::DDS::Time_t & source_timestamp) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::writedispose_w_timestamp(&instance_data, handle, source_timestamp);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataWriter_impl::get_key_value (
    IoTData & key_holder,
    ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::get_key_value(&key_holder, handle);
}

::DDS::InstanceHandle_t
DDS::IoT::IoTDataDataWriter_impl::lookup_instance (
    const DDS::IoT::IoTData & instance_data) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataWriter_impl::lookup_instance(&instance_data);
}

// DDS DDS::IoT::IoTData DataReader_impl Object Body
DDS::IoT::IoTDataDataReader_impl::IoTDataDataReader_impl ()
{
    // Parent constructor takes care of everything.
}

DDS::IoT::IoTDataDataReader_impl::~IoTDataDataReader_impl ()
{
    // Parent destructor takes care of everything.
}

DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::init (
    DDS::OpenSplice::Subscriber *subscriber,
    const DDS::DataReaderQos &qos,
    DDS::OpenSplice::TopicDescription *a_topic,
    const char *name,
    DDS::OpenSplice::cxxCopyIn copyIn,
    DDS::OpenSplice::cxxCopyOut copyOut)
{
    return DDS::OpenSplice::FooDataReader_impl::nlReq_init(
            subscriber, qos, a_topic, name, copyIn, copyOut,
            this->dataSeqAlloc, this->dataSeqLength, this->dataSeqGetBuffer, this->dataSeqCopyOut);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::read (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status = DDS::RETCODE_OK;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::read(&received_data, info_seq, max_samples, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::take (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status = DDS::RETCODE_OK;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::take(&received_data, info_seq, max_samples, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::read_w_condition (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status = DDS::RETCODE_OK;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::read_w_condition(&received_data, info_seq, max_samples, a_condition);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::take_w_condition (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status = DDS::RETCODE_OK;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::take_w_condition(&received_data, info_seq, max_samples, a_condition);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::read_next_sample (
    DDS::IoT::IoTData & received_data,
    ::DDS::SampleInfo & sample_info) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataReader_impl::read_next_sample(&received_data, sample_info);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::take_next_sample (
    DDS::IoT::IoTData & received_data,
    ::DDS::SampleInfo & sample_info) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataReader_impl::take_next_sample(&received_data, sample_info);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::read_instance (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::read_instance(&received_data, info_seq, max_samples, a_handle, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::take_instance (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::take_instance(&received_data, info_seq, max_samples, a_handle, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::read_next_instance (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::read_next_instance(&received_data, info_seq, max_samples, a_handle, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::take_next_instance (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::take_next_instance(&received_data, info_seq, max_samples, a_handle, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::read_next_instance_w_condition (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::read_next_instance_w_condition(&received_data, info_seq, max_samples, a_handle, a_condition);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::take_next_instance_w_condition (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReader_impl::take_next_instance_w_condition(&received_data, info_seq, max_samples, a_handle, a_condition);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::return_loan (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t result = ::DDS::RETCODE_OK;

    result = this->write_lock ();
    if (result == DDS::RETCODE_OK) {
        if ( received_data.length() > 0 ) {
            if (received_data.length() == info_seq.length() &&
                received_data.release() == info_seq.release() ) {
                if (!received_data.release()) {
                    result = DDS::OpenSplice::FooDataReader_impl::wlReq_return_loan( received_data.get_buffer(),
                                                                                     info_seq.get_buffer() );
                    if ( result == ::DDS::RETCODE_OK ) {
                        if ( !received_data.release() ) {
                            DDS::IoT::IoTDataSeq::freebuf( received_data.get_buffer(false) );
                            received_data.replace(0, 0, NULL, false);
                            ::DDS::SampleInfoSeq::freebuf( info_seq.get_buffer(false) );
                            info_seq.replace(0, 0, NULL, false);
                        }
                    }
                }
            } else {
                result = ::DDS::RETCODE_PRECONDITION_NOT_MET;
            }
        }
        this->unlock();
    }
    return result;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::get_key_value (
    DDS::IoT::IoTData & key_holder,
    ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataReader_impl::get_key_value(&key_holder, handle);
}

::DDS::InstanceHandle_t
DDS::IoT::IoTDataDataReader_impl::lookup_instance (
    const DDS::IoT::IoTData & instance) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataReader_impl::lookup_instance(&instance);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReader_impl::check_preconditions (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples)
{
    if ((max_samples < 0) && (max_samples != DDS::LENGTH_UNLIMITED)) {
        return DDS::RETCODE_BAD_PARAMETER;
    }

    /* Rule 1 : The values of len, max_len, and owns
     * for the two collections must be identical.
     */
    if ((received_data.length()  != info_seq.length())  ||
        (received_data.maximum() != info_seq.maximum()) ||
        (received_data.release() != info_seq.release()))
    {
        return DDS::RETCODE_PRECONDITION_NOT_MET;
    }

    /* Rule 4: If the input max_len>0 and the input owns==FALSE,
     * then the read operation will fail.
     */
    if ((info_seq.maximum() > 0) &&
        (info_seq.release() == false))
    {
        return DDS::RETCODE_PRECONDITION_NOT_MET;
    }

    /* Rule 5: If input max_len>0 and the input owns==TRUE,
     * then the read operation will...
     */
    if (info_seq.maximum() > 0) { /* owns is true, because of rule 4. */
        /* Rule 5a: If max_samples = LENGTH_UNLIMITED,
         * then at most max_len values will be copied.
         * Rule 5b: If max_samples <= max_len,
         * then at most max_samples values will be copied.
         */
        /* Rule 5c: If max_samples > max_len,
         * then the read operation will fail.
         */
        if ((max_samples != DDS::LENGTH_UNLIMITED) &&
            ((DDS::ULong)max_samples > info_seq.maximum())) {
            return DDS::RETCODE_PRECONDITION_NOT_MET;
        }
    }

    if ((max_samples == 0) ||
        ((info_seq.maximum() == 0) && (info_seq.release()))) {
        return DDS::RETCODE_NO_DATA;
    }

    return DDS::RETCODE_OK;
}

void *
DDS::IoT::IoTDataDataReader_impl::dataSeqAlloc (
    void * received_data,
    DDS::ULong len)
{
    DDS::IoT::IoTDataSeq *data_seq = reinterpret_cast<DDS::IoT::IoTDataSeq *>(received_data);
    data_seq->replace(len, len, data_seq->allocbuf(len), false);
    return data_seq->get_buffer();
}

void *
DDS::IoT::IoTDataDataReader_impl::dataSeqGetBuffer (
    void * received_data,
    DDS::ULong index)
{
	DDS::IoT::IoTDataSeq *data_seq = reinterpret_cast<DDS::IoT::IoTDataSeq *>(received_data);
	return &((*data_seq)[index]);
}

void
DDS::IoT::IoTDataDataReader_impl::dataSeqLength (
    void * received_data,
    DDS::ULong len)
{
    DDS::IoT::IoTDataSeq *data_seq = reinterpret_cast<DDS::IoT::IoTDataSeq *>(received_data);
    data_seq->length(len);
}

void
DDS::IoT::IoTDataDataReader_impl::dataSeqCopyOut (
    const void * from,
    void * to)
{
    DDS::IoT::IoTData *data = reinterpret_cast<DDS::IoT::IoTData *>(to);
    __DDS_IoT_IoTData__copyOut(from, data);
}

void
DDS::IoT::IoTDataDataReader_impl::copyDataOut (
    const void * from,
    void * to)
{
    DDS::IoT::IoTData *data = reinterpret_cast<DDS::IoT::IoTData *>(to);
    __DDS_IoT_IoTData__copyOut(from, data);
}


// DDS DDS::IoT::IoTData DataReaderView_impl Object Body
DDS::IoT::IoTDataDataReaderView_impl::IoTDataDataReaderView_impl ()
{
    // Parent constructor takes care of everything.
}

DDS::IoT::IoTDataDataReaderView_impl::~IoTDataDataReaderView_impl ()
{
    // Parent destructor takes care of everything.
}

DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::init (
    DDS::OpenSplice::DataReader *reader,
    const char *name,
    const DDS::DataReaderViewQos &qos,
    DDS::OpenSplice::cxxCopyIn copyIn,
    DDS::OpenSplice::cxxCopyOut copyOut)
{
    return DDS::OpenSplice::FooDataReaderView_impl::nlReq_init(
            reader, name, qos, copyIn, copyOut, DDS::IoT::IoTDataDataReader_impl::dataSeqAlloc,
            DDS::IoT::IoTDataDataReader_impl::dataSeqLength);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::read (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::read(&received_data, info_seq, max_samples, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::take (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::take(&received_data, info_seq, max_samples, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::read_w_condition (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::read_w_condition(&received_data, info_seq, max_samples, a_condition);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::take_w_condition (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::take_w_condition(&received_data, info_seq, max_samples, a_condition);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::read_next_sample (
    DDS::IoT::IoTData & received_data,
    ::DDS::SampleInfo & sample_info) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataReaderView_impl::read_next_sample(&received_data, sample_info);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::take_next_sample (
    DDS::IoT::IoTData & received_data,
    ::DDS::SampleInfo & sample_info) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataReaderView_impl::take_next_sample(&received_data, sample_info);
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::read_instance (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::read_instance(&received_data, info_seq, max_samples, a_handle, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::take_instance (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::take_instance(&received_data, info_seq, max_samples, a_handle, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::read_next_instance (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::read_next_instance(&received_data, info_seq, max_samples, a_handle, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::take_next_instance (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::SampleStateMask sample_states,
    ::DDS::ViewStateMask view_states,
    ::DDS::InstanceStateMask instance_states) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::take_next_instance(&received_data, info_seq, max_samples, a_handle, sample_states, view_states, instance_states);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::read_next_instance_w_condition (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::read_next_instance_w_condition(&received_data, info_seq, max_samples, a_handle, a_condition);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::take_next_instance_w_condition (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq,
    ::DDS::Long max_samples,
    ::DDS::InstanceHandle_t a_handle,
    ::DDS::ReadCondition_ptr a_condition) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t status;

    status = DDS::IoT::IoTDataDataReader_impl::check_preconditions(received_data, info_seq, max_samples);
    if ( status == ::DDS::RETCODE_OK ) {
        status = DDS::OpenSplice::FooDataReaderView_impl::take_next_instance_w_condition(&received_data, info_seq, max_samples, a_handle, a_condition);
    }
    return status;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::return_loan (
    DDS::IoT::IoTDataSeq & received_data,
    ::DDS::SampleInfoSeq & info_seq) THROW_ORB_EXCEPTIONS
{
    ::DDS::ReturnCode_t result = ::DDS::RETCODE_OK;

    result = this->write_lock ();
    if (result == DDS::RETCODE_OK) {
        if ( received_data.length() > 0 ) {
            if (received_data.length() == info_seq.length() &&
                received_data.release() == info_seq.release() ) {
                if (!received_data.release()) {
                    result = DDS::OpenSplice::FooDataReaderView_impl::wlReq_return_loan( received_data.get_buffer(),
                                                           info_seq.get_buffer() );

                    if ( result == ::DDS::RETCODE_OK ) {
                        if ( !received_data.release() ) {
                            DDS::IoT::IoTDataSeq::freebuf( received_data.get_buffer(false) );
                            received_data.replace(0, 0, NULL, false);
                            ::DDS::SampleInfoSeq::freebuf( info_seq.get_buffer(false) );
                            info_seq.replace(0, 0, NULL, false);
                        }
                    } else if ( result == ::DDS::RETCODE_NO_DATA ) {
                        if ( received_data.release() ) {
                            result = ::DDS::RETCODE_OK;
                        } else {
                            result = ::DDS::RETCODE_PRECONDITION_NOT_MET;
                        }
                    }
                }
            } else {
                result = ::DDS::RETCODE_PRECONDITION_NOT_MET;
            }
        }
        this->unlock();
    }
    return result;
}

::DDS::ReturnCode_t
DDS::IoT::IoTDataDataReaderView_impl::get_key_value (
    DDS::IoT::IoTData & key_holder,
    ::DDS::InstanceHandle_t handle) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataReaderView_impl::get_key_value(&key_holder, handle);
}

::DDS::InstanceHandle_t
DDS::IoT::IoTDataDataReaderView_impl::lookup_instance (
    const DDS::IoT::IoTData & instance) THROW_ORB_EXCEPTIONS
{
    return DDS::OpenSplice::FooDataReaderView_impl::lookup_instance(&instance);
}
