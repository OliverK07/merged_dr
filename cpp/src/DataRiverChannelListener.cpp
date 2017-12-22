/*
 *                         Vortex OpenSplice
 *
 *   This software and documentation are Copyright 2006 to 2017 ADLINK
 *   Technology Limited, its affiliated companies and licensors. All rights
 *   reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

#include "DataRiverChannelListener.h"
#include "CheckStatus.h"
#include <sstream>
#include "IOTP_GatewayClient.h"

using namespace DDS;
using namespace DDS::IoT;
using namespace Watson_IOTP;

// --------------------------------------------------------------------------------------------------------
//                                              ListenerDataListener                                     --
// --------------------------------------------------------------------------------------------------------
int ioTValueCount(IoTValue data)
{
   int result = 0;
   switch(data._d())
   {
      case TYPE_IoTUI8:
      case TYPE_IoTUI16:
      case TYPE_IoTUI32:
      case TYPE_IoTUI64:
      case TYPE_IoTI8:
      case TYPE_IoTI16:
      case TYPE_IoTI32:
      case TYPE_IoTI64:
      case TYPE_IoTF32:
      case TYPE_IoTF64:
      case TYPE_IoTB:
      case TYPE_IoTStr:
      case TYPE_IoTCh:
         result = 1;
         break;
      case TYPE_IoTUI8Seq:
         result = data.ui8Seq ().length ();
         break;
      case TYPE_IoTUI16Seq:
         result = data.ui16Seq ().length ();
         break;
      case TYPE_IoTUI32Seq:
         result = data.ui32Seq ().length ();
         break;
      case TYPE_IoTUI64Seq:
         result = data.ui64Seq ().length ();
         break;
      case TYPE_IoTI8Seq:
         result = data.i8Seq ().length ();
         break;
      case TYPE_IoTI16Seq:
         result = data.i16Seq ().length ();
         break;
      case TYPE_IoTI32Seq:
         result = data.i32Seq ().length ();
         break;
      case TYPE_IoTI64Seq:
         result = data.i64Seq ().length ();
         break;
      case TYPE_IoTF32Seq:
         result = data.f32Seq ().length ();
         break;
      case TYPE_IoTF64Seq:
         result = data.f64Seq ().length ();
         break;
      case TYPE_IoTBSeq:
         result = data.bSeq ().length ();
         break;
      case TYPE_IoTStrSeq:
         result = data.strSeq ().length ();
         break;
      case TYPE_IoTChSeq:
         result = data.chSeq ().length ();
         break;
      default:
         break;
   }
   return result;
}

std::string ioTValueToStringAt(IoTValue data, int pos)
{
   std::stringstream result;
   switch(data._d())
   {
      case TYPE_IoTUI8 :
         result << (IoTUI16) data.ui8 ();//IoTUI8
         break;
      case TYPE_IoTUI16 :
         result << (IoTUI16) data.ui16 ();
         break;
      case TYPE_IoTUI32 :
         result << (IoTUI32) data.ui32 ();
         break;
      case TYPE_IoTUI64 :
         result << (IoTUI64) data.ui64 ();
         break;
      case TYPE_IoTI8 :
         result << (IoTI16) data.i8 ();//IoTI8
         break;
      case TYPE_IoTI16 :
         result << (IoTI16) data.i16 ();
         break;
      case TYPE_IoTI32 :
         result << (IoTI32) data.i32 ();
         break;
      case TYPE_IoTI64 :
         result << (IoTI64) data.i64 ();
         break;
      case TYPE_IoTF32 :
         result << (IoTF32) data.f32 ();
         break;
      case TYPE_IoTF64 :
         result << (IoTF64) data.f64 ();
         break;
      case TYPE_IoTB :
         result << (IoTB) data.b ();
         break;
      case TYPE_IoTStr :
         result << (IoTStr) data.str ();
         break;
      case TYPE_IoTCh :
         result << (IoTCh) data.ch ();
         break;
      case TYPE_IoTUI8Seq:
         result << (IoTUI16) data.ui8Seq()[pos];//IoTUI8
         break;
      case TYPE_IoTUI16Seq:
         result << (IoTUI16) data.ui16Seq()[pos];
         break;
      case TYPE_IoTUI32Seq:
         result << (IoTUI32) data.ui32Seq()[pos];
         break;
      case TYPE_IoTUI64Seq:
         result << (IoTUI64) data.ui64Seq()[pos];
         break;
      case TYPE_IoTI8Seq:
         result << (IoTI16) data.i8Seq()[pos];//IoTI8
         break;
      case TYPE_IoTI16Seq:
         result << (IoTI16) data.i16Seq()[pos];
         break;
      case TYPE_IoTI32Seq:
         result << (IoTI32) data.i32Seq()[pos];
         break;
      case TYPE_IoTI64Seq:
         result << (IoTI64) data.i64Seq()[pos];
         break;
      case TYPE_IoTF32Seq:
         result << (IoTF32) data.f32Seq()[pos];
         break;
      case TYPE_IoTF64Seq:
         result << (IoTF64) data.f64Seq()[pos];
         break;
      case TYPE_IoTBSeq:
         result << (IoTB) data.bSeq()[pos];
         break;
      case TYPE_IoTStrSeq:
         result << (IoTStr) data.strSeq()[pos];
         break;
      case TYPE_IoTChSeq:
         result << (IoTCh) data.chSeq()[pos];
         break;
      default:
         break;
   }
   return result.str();
}

void DataRiverChannelListener::on_data_available (DDS::DataReader_ptr reader)
THROW_ORB_EXCEPTIONS
{
   DDS::ReturnCode_t status;
   IoTDataSeq dataSeq;
   SampleInfoSeq infoSeq;

   // Initial Watson Gateway Client
   IOTP_GatewayClient *m_watson_client;
   bool m_watson_status;
   std::string jsonMessage;
   m_watson_client = new IOTP_GatewayClient("gateway.cfg");
   try
   {
     m_watson_status = m_watson_client->connect();
   }
   catch (...)
   {
     cout << "connect failure with m_watson_client=" << m_watson_client << endl;
   }
   std::string D_value;
   std::string D_name;

   status = m_IoTDataReader->read(dataSeq, infoSeq, LENGTH_UNLIMITED,
         ANY_SAMPLE_STATE, NEW_VIEW_STATE, ANY_INSTANCE_STATE);
   checkStatus(status, "IoTDataReader::read");
   cout << "=== [DataRiverChannelListener::on_data_available] - dataSeq.length : " << dataSeq.length() << endl;

   for (DDS::ULong j = 0; j < dataSeq.length(); j++)
   {
      cout<<"\n    --- message received ---\n";
      cout<<"\n    instanceId  : " << dataSeq[j].instanceId.m_ptr << endl;
      cout<<"\n    typeName : " << dataSeq[j].typeName.m_ptr << endl;

      cout << "    IoTData :   " << endl;

      for(int i = 0; i < dataSeq[j].values.length(); i++)
      {
         cout << "    Name  : " << dataSeq[j].values[i].name.m_ptr << "    Value type : \""
              << dataSeq[j].values[i].value._d () << "\"" << endl;
         int noOfElement = ioTValueCount(dataSeq[j].values[i].value);
         for (int k = 0; k < noOfElement; k++)
         {
            cout << "    Value " << k << " : \"" << ioTValueToStringAt(dataSeq[j].values[i].value,k) << "\"" << endl;
            D_name = dataSeq[j].values[i].name.m_ptr;
            D_value = ioTValueToStringAt(dataSeq[j].values[i].value,k);
            jsonMessage = "{\"Data\": {\""+D_name+"\": \""+ D_value +"\" } }";
            cout << jsonMessage << endl;
            cout << "First publishing gateway event without listener :" << jsonMessage << "\n";
            m_watson_client->publishGatewayEvent("temp", "json", jsonMessage.c_str(), 0);
         }
      }
   }



   delete m_watson_client;
   status = m_IoTDataReader->return_loan(dataSeq, infoSeq);
   checkStatus(status, "MsgDataReader::return_loan");
   // unblock the waitset in Subscriber main loop
   m_guardCond->set_trigger_value(true);
};

void DataRiverChannelListener::on_requested_deadline_missed (DDS::DataReader_ptr
                                                             reader,
                                                             const DDS::RequestedDeadlineMissedStatus &status)
THROW_ORB_EXCEPTIONS
{
   cout<< "\n=== [ListenerDataListener::on_requested_deadline_missed] : triggered\n";
   cout<< "\n=== [ListenerDataListener::on_requested_deadline_missed] : stopping\n";
   m_closed = true;
   // unblock the waitset in Subscriber main loop
   m_guardCond->set_trigger_value(true);
};

void DataRiverChannelListener::on_requested_incompatible_qos (DDS::DataReader_ptr
                                                              reader,
                                                              const DDS::RequestedIncompatibleQosStatus &status)
THROW_ORB_EXCEPTIONS
{
   cout<<"\n=== [ListenerDataListener::on_requested_incompatible_qos] : triggered\n";
};

void DataRiverChannelListener::on_sample_rejected (DDS::DataReader_ptr reader, const DDS::SampleRejectedStatus &status)
THROW_ORB_EXCEPTIONS
{
   cout<<"\n=== [ListenerDataListener::on_sample_rejected] : triggered\n";
};

void DataRiverChannelListener::on_liveliness_changed (DDS::DataReader_ptr reader,
                                                      const DDS::LivelinessChangedStatus &status)
THROW_ORB_EXCEPTIONS
{
   cout<<"\n=== [ListenerDataListener::on_liveliness_changed] : triggered\n";
};

void DataRiverChannelListener::on_subscription_matched (DDS::DataReader_ptr reader,
                                                        const DDS::SubscriptionMatchedStatus &status)
THROW_ORB_EXCEPTIONS
{
   cout<<"\n=== [ListenerDataListener::on_subscription_matched] : triggered\n";
};

void DataRiverChannelListener::on_sample_lost (DDS::DataReader_ptr reader, const DDS
                                               ::SampleLostStatus &status)
THROW_ORB_EXCEPTIONS
{
   cout<<"\n=== [ListenerDataListener::on_sample_lost] : triggered\n";
};
