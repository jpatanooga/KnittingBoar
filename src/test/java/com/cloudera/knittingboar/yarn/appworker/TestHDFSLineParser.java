package com.cloudera.knittingboar.yarn.appworker;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.knittingboar.yarn.UpdateableInt;
import com.cloudera.knittingboar.yarn.appworker.HDFSLineParser;

import junit.framework.TestCase;
import static junit.framework.Assert.*;

public class TestHDFSLineParser extends TestCase {
  
  private Configuration conf;
  private Path testDir;
  private Path inputFile;
  private FileSystem localFs;
  
  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    localFs = FileSystem.getLocal(conf);
    testDir = new Path("testData");
    inputFile = new Path(testDir, "test.txt");

    Writer writer = new OutputStreamWriter(localFs.create(inputFile, true));
    writer.write("10000\n20000\n30000");
    writer.close();
  }
  
  @After
  public void cleanup() throws IOException {
    localFs.delete(testDir, true);
  }
  
  @Test
  public void testReadFullFile() throws IOException {
    HDFSLineParser<UpdateableInt> parser = new HDFSLineParser<UpdateableInt>(UpdateableInt.class);
    parser.setFile(inputFile.toString());
    
    List<UpdateableInt> records = new LinkedList<UpdateableInt>();

    parser.parse();
    
    while (parser.hasMoreRecords()) {
      UpdateableInt r = parser.nextRecord();
      records.add(r);
    }
    
    assertEquals(3, records.size());
    assertEquals(Integer.valueOf(10000), records.get(0).get());
    assertEquals(Integer.valueOf(20000), records.get(1).get());
    assertEquals(Integer.valueOf(30000), records.get(2).get());
  }
  
  @Test
  public void testReadPartialFileToEnd() throws IOException {
    HDFSLineParser<UpdateableInt> parser = new HDFSLineParser<UpdateableInt>(UpdateableInt.class);
    parser.setFile(inputFile.toString(), 4, 500);
    
    List<UpdateableInt> records = new LinkedList<UpdateableInt>();

    parser.parse();
    
    while (parser.hasMoreRecords()) {
      UpdateableInt r = parser.nextRecord();
      records.add(r);
    }
    
    assertEquals(2, records.size());
    assertEquals(Integer.valueOf(20000), records.get(0).get());
    assertEquals(Integer.valueOf(30000), records.get(1).get());
  }
  
  @Test
  public void testReadPartialFileToMid() throws IOException {
    HDFSLineParser<UpdateableInt> parser = new HDFSLineParser<UpdateableInt>(UpdateableInt.class);
    parser.setFile(inputFile.toString(), 4, 12);
    
    List<UpdateableInt> records = new LinkedList<UpdateableInt>();

    parser.parse();
    
    while (parser.hasMoreRecords()) {
      UpdateableInt r = parser.nextRecord();
      records.add(r);
    }
    
    assertEquals(1, records.size());
    assertEquals(Integer.valueOf(20000), records.get(0).get());
  }

  @Test
  public void testReadPartialFileToMidOverflow() throws IOException {
    HDFSLineParser<UpdateableInt> parser = new HDFSLineParser<UpdateableInt>(UpdateableInt.class);
    parser.setFile(inputFile.toString(), 4, 13);
    
    List<UpdateableInt> records = new LinkedList<UpdateableInt>();

    parser.parse();
    
    while (parser.hasMoreRecords()) {
      UpdateableInt r = parser.nextRecord();
      records.add(r);
    }
    
    assertEquals(2, records.size());
    assertEquals(Integer.valueOf(20000), records.get(0).get());
    assertEquals(Integer.valueOf(30000), records.get(1).get());
  }
}
