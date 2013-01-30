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
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory;
import com.cloudera.knittingboar.records.SVMLightUtils;

import junit.framework.TestCase;

public class TestConvert20NewsTestDataset extends TestCase {

  private static Path workDir20NewsLocal = new Path(new Path("/tmp"), "Dataset20Newsgroups");
  private static File unzipDir = new File( workDir20NewsLocal + "/20news-bydate");
  private static String strKBoarTrainDirInput = "" + unzipDir.toString() + "/KBoar-train/";
  private static String strKBoarSVMLightTrainDirInput = "" + unzipDir.toString() + "/KBoar-train-svmlight/";
/*
  public void testNaiveBayesFormatConverter() throws IOException {
        
//    int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles(strKBoarTrainDirInput, "/Users/jpatterson/Downloads/datasets/20news-kboar/train-dataset-unit-test/", 21000);
    File file20News = DataUtils.getTwentyNewsGroupDir();
    
    int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles( DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-train/", strKBoarTrainDirInput, 12000);
    
    //int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles("/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/", 2850);
    
    System.out.println( "Total: " + count );
    
    assertEquals( 11314, count );
    
  }  
  */
  
  
  public void testConvert20NewsToSVMLight() throws Exception {
    
//    File file20News = DataUtils.getTwentyNewsGroupDir();
    
  //  int count = DatasetConverter.ConvertNewsgroupsFromSingleFilesToSVMLightFormat( DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-train/", strKBoarSVMLightTrainDirInput, 12000);
    
    
    
    
    TwentyNewsgroupsRecordFactory rec_factory = new TwentyNewsgroupsRecordFactory("  ");
  
    String svmlight_rec_0 = "0 |f 7:4.3696374e-02 8:1.0872085e-01 19:2.2659289e-02 20:1.6952585e-02 50:3.3265986e-02 52:2.8914521e-02 99:6.2935837e-02 111:3.6749814e-02 124:4.5141779e-02 147:2.7024418e-02 153:5.3756956e-02 169:2.8062440e-02 182:4.7379807e-02 183:2.7668567e-02 188:1.4508039e-02 269:2.3687121e-02 271:2.0829555e-02 297:2.8352227e-02 311:3.3546336e-02 319:2.8875276e-02 332:4.7258154e-02 337:3.1720489e-02 360:6.8111412e-02 368:4.4445150e-02 411:4.4164777e-02 488:8.4059432e-02 586:2.9122708e-02 591:9.5403686e-02 664:3.6937956e-02 702:2.8176809e-02 737:1.6336726e-01 739:5.1228814e-02 757:3.4760747e-02 764:3.6367100e-02 768:6.1244022e-02 791:2.1772176e-01 817:7.4271448e-02 848:4.0480603e-02 895:5.1346138e-02 933:4.1986264e-02 979:1.1311502e-01 1003:4.5158323e-02 1005:4.0224314e-02 1021:4.6525169e-02 1071:2.9869374e-02 1127:2.1704819e-02 1133:4.5880664e-02 1162:3.8132094e-02 1178:5.2212182e-02 1180:1.0740499e-01 1338:4.9277205e-02 1360:4.6650354e-02 1498:5.9916675e-02 1511:7.6297082e-02 1577:5.0769087e-02 1659:5.0992116e-02 1666:2.4987224e-02 1674:2.9845037e-02 1810:4.6527624e-02 1966:4.3204561e-02 2018:4.3157250e-02 2066:1.3678090e-01 2074:1.0599699e-01 2117:9.8577492e-02 2183:1.4329165e-01 2248:1.2792459e-01 2276:7.9498030e-02 2316:4.9681831e-02 2340:5.8379412e-02 2762:5.1772792e-02 2771:4.9624689e-02 3077:2.1542890e-01 3227:8.3143584e-02 3246:5.2039523e-02 3282:5.2630566e-02 3369:7.0463479e-02 3615:5.6905646e-02 3620:6.6913836e-02 3962:6.1502680e-02 4132:2.1751978e-01 4143:2.6172629e-01 4144:9.1886766e-02 4499:1.1314832e-01 5031:7.9870239e-02 5055:8.6920090e-02 5401:5.4840717e-02 5423:9.5343769e-02 5860:8.9788958e-02 6065:8.6977042e-02 6668:7.6055169e-02 6697:6.8251781e-02 7139:6.4996362e-02 7426:1.2097790e-01 7606:1.9588335e-01 8870:1.4963643e-01 9804:9.4143294e-02 12121:7.4564628e-02 13942:1.6451047e-01 14595:1.0607405e-01 15422:8.9860193e-02 15652:1.0834268e-01 16223:9.6487328e-02 16859:1.0539885e-01 17424:8.1960648e-02 19529:9.3970060e-02 23299:1.8965191e-01 24377:1.0888006e-01 24448:9.8843329e-02 24454:2.8149781e-01 24455:2.1925208e-01 26622:1.0557952e-01 31771:1.3358803e-01 41700:1.2038895e-0";

    
    String line_0 = "comp.graphics  from sloan@cis.uab.edu kenneth sloan subject re surface normal orientations article id cis.1993apr6.181509.1973 organization cis university alabama birmingham lines 16 article 1993apr6.175117.1848@cis.uab.edu sloan@cis.uab.edu kenneth sloan writes brilliant algorithm seriously correct up sign change flaw obvious therefore shown sorry about kenneth sloan computer information sciences sloan@cis.uab.edu university alabama birmingham 205 934-2213 115a campbell hall uab station 205 934-5473 fax birmingham al 35294-1170 ";
    
    
    
    String line_1 = "comp.graphics  from";
/*
    String line_2 = "comp.os.ms-windows.misc  from gamet@erg.sri.com thomas gamet subject keyboard specifications organization sri international menlo park ca lines 35 all hardware firmware gurus my current home project build huge paddle keyboard physically handicapped relative mine my goal keyboard look exactly like sytle keyboard its host system highly endowed keyboard little pcl from z world its heart only thing i lack detailed information hardware signaling 486 windows 3.1 dos 5.0 expecting my project independant windows my hope some you fellow window users programmers recognize what i need willing point me right direction i have winn l rosch hardware bible 2nd edition hb gives most all information i need concerning scan codes even wire diagram ps/2 style connector i need leaves number important questions unanswered 1 synchronous asynchronous serial communication i'm guessing synchronous since host providing clock either event how data framed 2 half duplex truly one way i'm guessing half duplex since host can turn leds off 3 any chipsets available communicating keyboard standard other than cannibalizing real keyboard anyone knows book article any other written source information above please advise me gamet@erg.sri.com whatever i do must safe i cannot afford replace 486 event booboo thank you your time danke fuer ihre zeit thomas gamet gamet@erg.sri.com software engineer sri international ";

    String line_3 = "misc.forsale  from cmd@cbnewsc.cb.att.com craig.m.dinsmore subject vcr cassette generator tube tester lawn spreader organization at&t distribution chi keywords forsale lines 21 sale vcr samsung vr2610 basic 2 head machine has problem loading tape otherwise plays records just fine remote missing 25 make offer cassette deck pioneer ct-f900 three head servo control dolby top line close several years ago rewind doesn't work well all else fine service owners manual included 45 offer generator 120 vac 2000-2500 watt has voltmeter w duplex outlet 5 hp engine should drive full output manufactered master mechanic burlington wisconsin 50 make offer eico model 625 tube tester 20 make offer lawn spreader scott precision flow model pf-1 drop type excellent condition ideal smaller yard 20 make offer craig days 979-0059 home 293-5739 ";
*/
    
    
    
    
    Vector v_0 = new RandomAccessSparseVector( TwentyNewsgroupsRecordFactory.FEATURES );
    
    int actual_0 = rec_factory.processLine(line_1, v_0);
    
    Utils.PrintVectorNonZero(v_0);
    String res = SVMLightUtils.ConvertVectorIntoSVMLightFormat(v_0, "1");
    
    System.out.println(res);
    
    
    
  }
  
  
}
