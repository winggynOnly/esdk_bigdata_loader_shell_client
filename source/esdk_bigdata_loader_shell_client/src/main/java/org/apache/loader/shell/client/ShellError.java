/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.loader.shell.client;

import org.apache.sqoop.common.ErrorCode;

public enum ShellError implements ErrorCode
{
    /** The parameter is empty **/
    PARAMETER_EMPTY("The parameter is empty"),
    
    /** The parameter is null **/
    PARAMETER_NULL("The parameter is null"),
    
    CONFIGURATION_INCORRECT("The configuartion file is incorrect."),
    
    LOAD_CONF_FILE_FAILURE("Failed to load the configuration file"),
    
    /** Failed to submit job **/
    SUBMIT_JOB_FAILED("Failed to submit job"),
    
    /** The options is invalid **/
    OPTIONS_INVALID("The options is invalid"),
    
    /** Failed to login the kerberos */
    LOGIN_FAILURE("Failed to login the kerberos"),
   
    /** Failed to execute the mapred reduce job */
    JOB_FAILURE("Failed to execute the mapred reduce job"),
    
    /** Failed to operate phoenix table */
    OPERATE_PHOENIX_FAILURE("Failed to operate phoenix table"),
    
    /** Failed to check line value */
    CHECK_LINE_FAILURE("Failed to check line value"),
    
    /** The file already exist */
    FILE_ALREADY_EXIST("The file already exist"),
    
    /** The file not exist */
    FILE_NOT_EXIST("The file not exist"),
    
    /** Failed to step */
    FAILED_TO_STEP("Failed to step"),
    ;
    
    private final String message;
    
    private ShellError(String message)
    {
        this.message = message;
    }
    
    public String getCode()
    {
        return name();
    }
    
    public String getMessage()
    {
        return message;
    }
}
