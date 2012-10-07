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

import com.cloudera.wovenwabbit.yarn.appworker.HDFSLineParser;

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
    writer.write("line1\nline2\nline3");
    writer.close();
  }
  
  @After
  public void cleanup() throws IOException {
    localFs.delete(testDir, true);
  }
  
  @Test
  public void testReadFullFile() throws IOException {
    HDFSLineParser parser = new HDFSLineParser();
    parser.setFile(inputFile.toString());
    
    List<String> records = new LinkedList<String>();

    parser.parse();
    
    while (parser.hasMoreRecords()) {
      String r = parser.nextRecord();
      records.add(r);
    }
    
    assertEquals(3, records.size());
    assertEquals("line1", records.get(0));
    assertEquals("line2", records.get(1));
    assertEquals("line3", records.get(2));
  }
  
  @Test
  public void testReadPartialFileToEnd() throws IOException {
    HDFSLineParser parser = new HDFSLineParser();
    parser.setFile(inputFile.toString(), 4, 500);
    
    List<String> records = new LinkedList<String>();

    parser.parse();
    
    while (parser.hasMoreRecords()) {
      String r = parser.nextRecord();
      records.add(r);
    }
    
    assertEquals(2, records.size());
    assertEquals("line2", records.get(0));
    assertEquals("line3", records.get(1));
  }
  
  @Test
  public void testReadPartialFileToMid() throws IOException {
    HDFSLineParser parser = new HDFSLineParser();
    parser.setFile(inputFile.toString(), 4, 12);
    
    List<String> records = new LinkedList<String>();

    parser.parse();
    
    while (parser.hasMoreRecords()) {
      String r = parser.nextRecord();
      records.add(r);
    }
    
    assertEquals(1, records.size());
    assertEquals("line2", records.get(0));
  }

  @Test
  public void testReadPartialFileToMidOverflow() throws IOException {
    HDFSLineParser parser = new HDFSLineParser();
    parser.setFile(inputFile.toString(), 4, 13);
    
    List<String> records = new LinkedList<String>();

    parser.parse();
    
    while (parser.hasMoreRecords()) {
      String r = parser.nextRecord();
      records.add(r);
    }
    
    assertEquals(2, records.size());
    assertEquals("line2", records.get(0));
    assertEquals("line3", records.get(1));
  }
}
