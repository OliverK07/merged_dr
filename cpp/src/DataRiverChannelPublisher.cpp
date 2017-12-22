/*************************************************************************
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

/************************************************************************
 * LOGICAL_NAME:    ListenerDataPublisher.cpp
 * FUNCTION:        OpenSplice Tutorial example code.
 * MODULE:          Tutorial for the C++ programming language.
 * DATE             September 2010.
 ************************************************************************
 *
 * This file contains the implementation for the 'ListenerDataPublisher' executable.
 *
 ***/
#include <string>
#include <sstream>
#include <iostream>
#include "DDSEntityManager.h"
#include "ccpp_IoTData.h"
#include "vortex_os.h"

#include "example_main.h"

using namespace DDS;
using namespace DDS::IoT;

int OSPL_MAIN (int argc, char *argv[])
{
  os_time delay_2s = { 2, 0 };
  DDSEntityManager mgr;

  // create domain participant
  char partition_name[] = "GenericIoTDDSConnector";
  mgr.createParticipant(partition_name);

  //create type
  IoTDataTypeSupport_var mt = new IoTDataTypeSupport();
  mgr.registerType(mt.in());

  //create Topic
  char topic_name[] = "GenericIoTData";
  mgr.createTopic(topic_name);

  //create Publisher
  mgr.createPublisher();

  // create DataWriter
  mgr.createWriter();

  // Publish Events
  DataWriter_var dwriter = mgr.getWriter();
  IoTDataDataWriter_var ioTDataWriter = IoTDataDataWriter::_narrow(dwriter.in());

  IoTData ioTDataInstance; /* Example on Stack */
  ioTDataInstance.typeName = DDS::string_dup("light control");
  ioTDataInstance.instanceId = DDS::string_dup("light1");

  ioTDataInstance.values.length(3);// alloc inside
  //ioTDataInstance.values.allocbuf(3);

  IoTUI8Seq uIT8SeqTemp(5);//contruct with max is 5. Mean that, memory alloc 5 bytes
  uIT8SeqTemp.length(3);//set length 3. 3 bytes was allocted to ready
  uIT8SeqTemp[0] = 10;
  uIT8SeqTemp[1] = 20;
  uIT8SeqTemp[2] = 30;

  ioTDataInstance.values[0].name = "Dimmer 0";
  ioTDataInstance.values[0].value._d(TYPE_IoTUI8);
  ioTDataInstance.values[0].value.ui8(40);

  ioTDataInstance.values[1].name = "Dimmer 1";
  ioTDataInstance.values[1].value._d(TYPE_IoTUI8Seq);
  ioTDataInstance.values[1].value.ui8Seq(uIT8SeqTemp);

  ioTDataInstance.values[2].name = "Dimmer 2";
  ioTDataInstance.values[2].value._d(TYPE_IoTUI8Seq);
  ioTDataInstance.values[2].value.ui8Seq(uIT8SeqTemp);

  cout << "=== [ListenerDataPublisher] writing a message containing :" << endl;
  cout << "    typeName  : " << ioTDataInstance.typeName.m_ptr << endl;
  cout << "    instanceId : \"" << ioTDataInstance.instanceId.m_ptr << "\"" << endl;
  cout << "    IoTData :   " << endl;
  cout << "    Name  : " << ioTDataInstance.values[0].name.m_ptr << "    Value type : \""
        << ioTDataInstance.values[0].value._d () << "\""
        << "    Value : \"" << (int)ioTDataInstance.values[0].value.ui8 () << "\"" << endl;

  cout << "    Name  : " << ioTDataInstance.values[1].name.m_ptr << "    Value type : \""
       << ioTDataInstance.values[1].value._d ()<< "\""<<endl;
  for (int i = 0; i < ioTDataInstance.values[1].value.ui8Seq ().length (); i++)
  {
     cout << "    Value " << i << " : \"" << (int)ioTDataInstance.values[1].value.ui8Seq()[i] << "\"" << endl;
  }

  cout << "    Name  : " << ioTDataInstance.values[2].name.m_ptr << "    Value type : \""
       << ioTDataInstance.values[2].value._d ()<< "\""<<endl;
  for (int i = 0; i < ioTDataInstance.values[2].value.ui8Seq ().length (); i++)
  {
     cout << "    Value " << i << " : \"" << (int)ioTDataInstance.values[2].value.ui8Seq()[i] << "\"" << endl;
  }

  ReturnCode_t status = ioTDataWriter->write(ioTDataInstance, NULL);
  checkStatus(status, "ioTDataWriter::write");
  os_nanoSleep(delay_2s);

  /* Remove the DataWriters */
  mgr.deleteWriter(ioTDataWriter.in ());

  /* Remove the Publisher. */
  mgr.deletePublisher();

  /* Remove the Topics. */
  mgr.deleteTopic();

  /* Remove Participant. */
  mgr.deleteParticipant();

  return 0;
}
