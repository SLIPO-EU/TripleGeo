/*
 * @(#) Configuration.java 	 version 1.3   3/11/2017
 *
 * Copyright (C) 2013-2017 Information Systems Management Institute, Athena R.C., Greece.
 *
 * This library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package eu.slipo.athenarc.triplegeo.utils;

/**
 * Class to handle exceptions raised by TripleGeo utilities. Added support for issuing exit codes to the operation system.
 * Created by: Kostas Patroumpas, 3/11/2017
 * Last modified: 24/11/2017 
 */

public class ExceptionHandler {

    public static void invoke(Exception e, String msg) {
            e.printStackTrace();
            System.err.println("Transformation process terminated abnormally. " + msg);
            System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
    }
    
}
