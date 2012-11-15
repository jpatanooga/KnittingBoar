/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.knittingboar.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.mahout.math.Vector;

public class Utils {
  
  public static void UnTarAndZipGZFile(final File inputFile,
      final File outputDir) throws FileNotFoundException, IOException,
      ArchiveException {
    
    unTar(inputFile, outputDir);
    unGzip(inputFile, outputDir);
    
  }
  
  /**
   * Untar an input file into an output file.
   * 
   * The output file is created in the output folder, having the same name as
   * the input file, minus the '.tar' extension.
   * 
   * @param inputFile
   *          the input .tar file
   * @param outputDir
   *          the output directory file.
   * @throws IOException
   * @throws FileNotFoundException
   * 
   * @return The {@link List} of {@link File}s with the untared content.
   * @throws ArchiveException
   */
  private static List<File> unTar(final File inputFile, final File outputDir)
      throws FileNotFoundException, IOException, ArchiveException {
    
    System.out.println(String.format("Untaring %s to dir %s.", inputFile
        .getAbsolutePath(), outputDir.getAbsolutePath()));
    
    final List<File> untaredFiles = new LinkedList<File>();
    final InputStream is = new FileInputStream(inputFile);
    final TarArchiveInputStream debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory()
        .createArchiveInputStream("tar", is);
    TarArchiveEntry entry = null;
    while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
      final File outputFile = new File(outputDir, entry.getName());
      if (entry.isDirectory()) {
        System.out.println(String.format(
            "Attempting to write output directory %s.", outputFile
                .getAbsolutePath()));
        if (!outputFile.exists()) {
          System.out.println(String.format(
              "Attempting to create output directory %s.", outputFile
                  .getAbsolutePath()));
          if (!outputFile.mkdirs()) {
            throw new IllegalStateException(String.format(
                "Couldn't create directory %s.", outputFile.getAbsolutePath()));
          }
        }
      } else {
        System.out.println(String.format("Creating output file %s.", outputFile
            .getAbsolutePath()));
        final OutputStream outputFileStream = new FileOutputStream(outputFile);
        IOUtils.copy(debInputStream, outputFileStream);
        outputFileStream.close();
      }
      untaredFiles.add(outputFile);
    }
    debInputStream.close();
    
    return untaredFiles;
  }
  
  /**
   * Ungzip an input file into an output file.
   * <p>
   * The output file is created in the output folder, having the same name as
   * the input file, minus the '.gz' extension.
   * 
   * @param inputFile
   *          the input .gz file
   * @param outputDir
   *          the output directory file.
   * @throws IOException
   * @throws FileNotFoundException
   * 
   * @return The {@File} with the ungzipped content.
   */
  private static File unGzip(final File inputFile, final File outputDir)
      throws FileNotFoundException, IOException {
    
    System.out.println(String.format("Ungzipping %s to dir %s.", inputFile
        .getAbsolutePath(), outputDir.getAbsolutePath()));
    
    final File outputFile = new File(outputDir, inputFile.getName().substring(
        0, inputFile.getName().length() - 3));
    
    final GZIPInputStream in = new GZIPInputStream(new FileInputStream(
        inputFile));
    final FileOutputStream out = new FileOutputStream(outputFile);
    
    for (int c = in.read(); c != -1; c = in.read()) {
      out.write(c);
    }
    
    in.close();
    out.close();
    
    return outputFile;
  }
  
  public static void PrintVector(Vector v) {
    
    boolean first = true;
    Iterator<Vector.Element> nonZeros = v.iterator();
    while (nonZeros.hasNext()) {
      Vector.Element vec_loc = nonZeros.next();
      
      if (!first) {
        System.out.print(",");
      } else {
        first = false;
      }
      
      System.out.print(" " + vec_loc.get());
      
    }
    
    System.out.println("");
    
  }
  
  public static void PrintVectorSection(Vector v, int num) {
    
    boolean first = true;
    Iterator<Vector.Element> nonZeros = v.iterator();
    int cnt = 0;
    
    while (nonZeros.hasNext()) {
      Vector.Element vec_loc = nonZeros.next();
      
      if (!first) {
        System.out.print(",");
      } else {
        first = false;
      }
      
      System.out.print(" " + vec_loc.get());
      if (cnt > num) {
        break;
      }
      cnt++;
    }
    
    System.out.println(" ######## ");
    
  }
  
  public static void PrintVectorNonZero(Vector v) {
    
    boolean first = true;
    Iterator<Vector.Element> nonZeros = v.iterateNonZero();
    while (nonZeros.hasNext()) {
      Vector.Element vec_loc = nonZeros.next();
      
      if (!first) {
        System.out.print(",");
      } else {
        first = false;
      }
      System.out.print(" " + vec_loc.get());
      
    }
    
    System.out.println("");
    
  }
  
  public static void PrintVectorSectionNonZero(Vector v, int size) {
    
    boolean first = true;
    Iterator<Vector.Element> nonZeros = v.iterateNonZero();
    
    int cnt = 0;
    
    while (nonZeros.hasNext()) {
      Vector.Element vec_loc = nonZeros.next();
      
      if (!first) {
        System.out.print(",");
      } else {
        first = false;
      }
      System.out.print(" " + vec_loc.get());
      
      if (cnt > size) {
        break;
      }
      cnt++;
    }
    
    System.out.println("");
    
  }
  
}
