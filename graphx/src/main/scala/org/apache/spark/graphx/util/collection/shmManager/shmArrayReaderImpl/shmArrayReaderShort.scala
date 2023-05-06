/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.util.collection.shmManager.shmArrayReaderImpl

import org.apache.spark.graphx.util.collection.shmManager.shmArrayReader

class shmArrayReaderShort(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize) {

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Short" + identifier)
  }

  var shmName : String = fullName

  type T = Short

  var dataTypeSize : Int = 2

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Short] = {
    val t = buffer.asShortBuffer()
    val arrayTest = new Array[Short](t.remaining())
    t.get(arrayTest)
    arrayTest

  }

  def shmArrayReaderGetByIndex(index: Int) : Short = {
    require(index < preAllocateSize,
      "Index " + index + " out of size " + preAllocateSize + " !")
    val t = buffer.asShortBuffer()
    t.get(index)
  }

}
