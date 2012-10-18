package com.cloudera.knittingboar.records;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.io.TestInputRecordsSplit;
import com.cloudera.knittingboar.utils.Utils;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

import junit.framework.TestCase;

public class TestTwentyNewsgroupsRecordFactory extends TestCase {

  
  
  

  private static final Log LOG = LogFactory.getLog(TestTwentyNewsgroupsRecordFactory.class.getName());

  private static int MAX_LENGTH = 1000;
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  
  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/train2/"));
  
  public void testLineParse() {
    
    
    String line = "comp.graphics  from sloan@cis.uab.edu kenneth sloan subject re surface normal orientations article id cis.1993apr6.181509.1973 organization cis university alabama birmingham lines 16 article 1993apr6.175117.1848@cis.uab.edu sloan@cis.uab.edu kenneth sloan writes brilliant algorithm seriously correct up sign change flaw obvious therefore shown sorry about kenneth sloan computer information sciences sloan@cis.uab.edu university alabama birmingham 205 934-2213 115a campbell hall uab station 205 934-5473 fax birmingham al 35294-1170 ";

    String[] parts = line.split(" ");
    
    System.out.println( "len: " + parts.length );
    
    System.out.println( parts[0] );
    
    
    
    String line2 = "comp.windows.x  from";

    
    String[] parts2 = line2.split(" ");
    
    System.out.println( "len: " + parts2.length );
    
    System.out.println( parts2[0] );
    
    
  }
  
  public void testRecordFactory() throws Exception {
    
    TwentyNewsgroupsRecordFactory rec_factory = new TwentyNewsgroupsRecordFactory("  ");
    
    String line_0 = "comp.graphics  from sloan@cis.uab.edu kenneth sloan subject re surface normal orientations article id cis.1993apr6.181509.1973 organization cis university alabama birmingham lines 16 article 1993apr6.175117.1848@cis.uab.edu sloan@cis.uab.edu kenneth sloan writes brilliant algorithm seriously correct up sign change flaw obvious therefore shown sorry about kenneth sloan computer information sciences sloan@cis.uab.edu university alabama birmingham 205 934-2213 115a campbell hall uab station 205 934-5473 fax birmingham al 35294-1170 ";
    
    String line_1 = "comp.graphics  from ab@nova.cc.purdue.edu allen b subject re fractals what good organization purdue university lines 16 article mdpyssc.2@fs1.mcc.ac.uk mdpyssc@fs1.mcc.ac.uk sue cunningham writes we have been using iterated systems compression board compress pathology images getting ratios 40 1 70 1 without too much loss quality taking about 4 mins per image compress 25mhz 486 decompression almost real time 386 software alone how does compare jpeg same images hardware far size speed image quality concerned despite my skeptical sometimes nearly rabid postings criticizing barnsley company i am very interested technique i weren't i probably wouldn't so critical ab ";

    String line_2 = "comp.os.ms-windows.misc  from gamet@erg.sri.com thomas gamet subject keyboard specifications organization sri international menlo park ca lines 35 all hardware firmware gurus my current home project build huge paddle keyboard physically handicapped relative mine my goal keyboard look exactly like sytle keyboard its host system highly endowed keyboard little pcl from z world its heart only thing i lack detailed information hardware signaling 486 windows 3.1 dos 5.0 expecting my project independant windows my hope some you fellow window users programmers recognize what i need willing point me right direction i have winn l rosch hardware bible 2nd edition hb gives most all information i need concerning scan codes even wire diagram ps/2 style connector i need leaves number important questions unanswered 1 synchronous asynchronous serial communication i'm guessing synchronous since host providing clock either event how data framed 2 half duplex truly one way i'm guessing half duplex since host can turn leds off 3 any chipsets available communicating keyboard standard other than cannibalizing real keyboard anyone knows book article any other written source information above please advise me gamet@erg.sri.com whatever i do must safe i cannot afford replace 486 event booboo thank you your time danke fuer ihre zeit thomas gamet gamet@erg.sri.com software engineer sri international ";

    String line_3 = "misc.forsale  from cmd@cbnewsc.cb.att.com craig.m.dinsmore subject vcr cassette generator tube tester lawn spreader organization at&t distribution chi keywords forsale lines 21 sale vcr samsung vr2610 basic 2 head machine has problem loading tape otherwise plays records just fine remote missing 25 make offer cassette deck pioneer ct-f900 three head servo control dolby top line close several years ago rewind doesn't work well all else fine service owners manual included 45 offer generator 120 vac 2000-2500 watt has voltmeter w duplex outlet 5 hp engine should drive full output manufactered master mechanic burlington wisconsin 50 make offer eico model 625 tube tester 20 make offer lawn spreader scott precision flow model pf-1 drop type excellent condition ideal smaller yard 20 make offer craig days 979-0059 home 293-5739 ";

    
    
    
    
    Vector v_0 = new RandomAccessSparseVector( TwentyNewsgroupsRecordFactory.FEATURES );
    
    int actual_0 = rec_factory.processLine(line_0, v_0);

    
    Vector v_1 = new RandomAccessSparseVector( TwentyNewsgroupsRecordFactory.FEATURES );
    
    int actual_1 = rec_factory.processLine(line_1, v_1);
    
    
    Vector v_2 = new RandomAccessSparseVector( TwentyNewsgroupsRecordFactory.FEATURES );
    
    int actual_2 = rec_factory.processLine(line_2, v_2);

    
    
    
    
    Vector v_3 = new RandomAccessSparseVector( TwentyNewsgroupsRecordFactory.FEATURES );
    
    int actual_3 = rec_factory.processLine(line_3, v_3);
    
    //Utils.PrintVector(v_3);
    
    
    
    
  }
  
  public void testInternalClassRepresentations() {
    
    TwentyNewsgroupsRecordFactory rec_factory = new TwentyNewsgroupsRecordFactory("\t");
    
    int class_id_0 = rec_factory.LookupIDForNewsgroupName("comp.graphics");
    
    assertEquals( 1, class_id_0 );
    
    int class_id_1 = rec_factory.LookupIDForNewsgroupName("sci.electronics");
    
    assertEquals( 12, class_id_1 );
    
    
  }
  
  public void testRecordFactoryOnDatasetShard() throws Exception {
    
    TwentyNewsgroupsRecordFactory rec_factory = new TwentyNewsgroupsRecordFactory("\t");
    //rec_factory.setClassSplitString("\t");
    
    
    
    JobConf job = new JobConf(defaultConf);
    Path file = new Path(workDir, "20news-part-0.txt");

    int tmp_file_size = 200000;
    
    long block_size = localFs.getDefaultBlockSize();
    
    System.out.println("default block size: " + (block_size / 1024 / 1024) + "MB");
    
    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
 
    FileInputFormat.setInputPaths(job, workDir);


      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);
      LongWritable key = new LongWritable();
      Text value = new Text();

      int numSplits = 1;
      
      InputSplit[] splits = format.getSplits(job, numSplits);
      
      LOG.info("requested " + numSplits + " splits, splitting: got =        " + splits.length);
      
      System.out.println( "---- debug splits --------- " );
      
      rec_factory.Debug();
      
      int total_read = 0;
      
      long ts_start = System.currentTimeMillis();
      
      for (int x = 0; x < splits.length; x++) {
        
        System.out.println( "> Split [" + x + "]: " + splits[x].getLength()  );
        
        
        int count = 0;
        InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[x]);
        while ( custom_reader.next(value)) {
          
          
          Vector v = new RandomAccessSparseVector(TwentyNewsgroupsRecordFactory.FEATURES);
          rec_factory.processLine(value.toString(), v);
          
          
          count++;
          //break;
          
        }
        
        System.out.println( "read: " + count + " records for split " + x );
 
        total_read += count;
        
      } // for each split
      
      long ts_total = System.currentTimeMillis() - ts_start;
      
      double vectors_per_sec =  (double)total_read / ((double)ts_total / 1000);
      
      System.out.println( "Time: " + ts_total  );
      
      System.out.println( "total recs read across all splits: " + total_read );
      
      System.out.println( "Vectors converted / sec: " + vectors_per_sec );
      
      assertEquals( total_read, 11314 );
      
      rec_factory.Debug();
      
    
  }
 
  
  public void testGetGroupByID() {
 
    
    TwentyNewsgroupsRecordFactory rec_factory = new TwentyNewsgroupsRecordFactory("\t");
    
    int id = rec_factory.LookupIDForNewsgroupName("rec.autos");
    
    String name = rec_factory.GetNewsgroupNameByID(id);
    
    System.out.println( ">>> " + name );

    int id2 = rec_factory.LookupIDForNewsgroupName("rec.motorcycles");
    
    String name2 = rec_factory.GetNewsgroupNameByID(id2);
    
    System.out.println( ">>> " + name2 );
  }
  
  
}
