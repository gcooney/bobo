package com.browseengine.bobo.geosearch.index.impl;

/*
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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.GeoIndexWriter;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.UpgradeIndexMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.CommandLineUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;

import com.browseengine.bobo.geosearch.bo.GeoSearchConfig;
import com.browseengine.bobo.geosearch.store.BlockGeoDeleteDirectory;

/**
 * Adapted from {@link IndexUpgrader} with slight modifications to support upgrading 
 * to lucene 4.x without losing the .geo segments.  Custom geo configurations are not supported from
 * the command line
 *
 */
public final class GeoIndexUpgrader {

    private static void printUsage() {
        System.err.println("Upgrades an index so all segments created with a previous Lucene version are rewritten.");
        System.err.println("Usage:");
        System.err.println("  java " + GeoIndexUpgrader.class.getName() + " [-delete-prior-commits] [-verbose] [-dir-impl X] indexDir");
        System.err.println("This tool keeps only the last commit in an index; for this");
        System.err.println("reason, if the incoming index has more than one commit, the tool");
        System.err.println("refuses to run by default. Specify -delete-prior-commits to override");
        System.err.println("this, allowing the tool to delete all but the last commit.");
        System.err.println("Specify a " + FSDirectory.class.getSimpleName() + 
            " implementation through the -dir-impl option to force its use. If no package is specified the " 
            + FSDirectory.class.getPackage().getName() + " package will be used.");
        System.err.println("WARNING: This tool may reorder document IDs!");
        System.exit(1);
    }

    /** Main method to run {code IndexUpgrader} from the
     *  command-line. */
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException {
        String path = null;
        boolean deletePriorCommits = false;
        PrintStream out = null;
        String dirImpl = null;
        int i = 0;
        while (i<args.length) {
            String arg = args[i];
            if ("-delete-prior-commits".equals(arg)) {
                deletePriorCommits = true;
            } else if ("-verbose".equals(arg)) {
                out = System.out;
            } else if (path == null) {
                path = arg;
            } else if ("-dir-impl".equals(arg)) {
                if (i == args.length - 1) {
                    System.out.println("ERROR: missing value for -dir-impl option");
                    System.exit(1);
                }
                i++;
                dirImpl = args[i];
            }else {
                printUsage();
            }
            i++;
        }
        if (path == null) {
            printUsage();
        }
    
        Directory dir = null;
        if (dirImpl == null) {
            dir = FSDirectory.open(new File(path));
        } else {
            dir = CommandLineUtil.newFSDirectory(dirImpl, new File(path));
        }
        new GeoIndexUpgrader(dir, Version.LUCENE_CURRENT, out, deletePriorCommits).upgrade();
    }
  
    private final Directory dir;
    private final IndexWriterConfig iwc;
    private final boolean deletePriorCommits;
    private GeoSearchConfig geoConfig;
  
    /** Creates index upgrader on the given directory, using an {@link IndexWriter} using the given
     * {@code matchVersion}. The tool refuses to upgrade indexes with multiple commit points. */
    public GeoIndexUpgrader(Directory dir, Version matchVersion) {
        this(dir, new IndexWriterConfig(matchVersion, null), new GeoSearchConfig(), false);
    }
  
    /** Creates index upgrader on the given directory, using an {@link IndexWriter} using the given
     * {@code matchVersion}. You have the possibility to upgrade indexes with multiple commit points by removing
     * all older ones. If {@code infoStream} is not {@code null}, all logging output will be sent to this stream. */
    public GeoIndexUpgrader(Directory dir, Version matchVersion, PrintStream infoStream, boolean deletePriorCommits) {
        this(dir, new IndexWriterConfig(matchVersion, null).setInfoStream(infoStream), new GeoSearchConfig(), 
                deletePriorCommits);
    }
  
    /** Creates index upgrader on the given directory, using an {@link IndexWriter} using the given
     * config. You have the possibility to upgrade indexes with multiple commit points by removing
     * all older ones. */
    public GeoIndexUpgrader(Directory dir, IndexWriterConfig iwc, GeoSearchConfig geoConfig, 
            boolean deletePriorCommits) {
        this.dir = dir;
        this.iwc = iwc;
        this.geoConfig = geoConfig;
        this.deletePriorCommits = deletePriorCommits;
    }

    /** Perform the upgrade. */
    public void upgrade() throws IOException {
        if (!DirectoryReader.indexExists(dir)) {
            throw new IndexNotFoundException(dir.toString());
        }
  
        if (!deletePriorCommits) {
            final Collection<IndexCommit> commits = DirectoryReader.listCommits(dir);
            if (commits.size() > 1) {
                throw new IllegalArgumentException("This tool was invoked to not delete prior commit points, but the following commits were found: " + commits);
            }
        }
    
        final IndexWriterConfig c = iwc.clone();
        c.setMergePolicy(new UpgradeIndexMergePolicy(c.getMergePolicy()));
        c.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    
        InfoStream infoStream = c.getInfoStream();
        final GeoIndexWriter w = new GeoIndexWriter(new BlockGeoDeleteDirectory(dir), c, geoConfig);
        try {
            if (infoStream.isEnabled("IndexUpgrader")) {
                infoStream.message("IndexUpgrader", "Upgrading all pre-" + Constants.LUCENE_MAIN_VERSION + " segments of index directory '" + dir + "' to version " + Constants.LUCENE_MAIN_VERSION + "...");
            }
            w.forceMerge(1);
            if (infoStream.isEnabled("IndexUpgrader")) {
                infoStream.message("IndexUpgrader", "All segments upgraded to version " + Constants.LUCENE_MAIN_VERSION);
            }
        } finally {
            w.close();
        }
    
        if (infoStream.isEnabled("IndexUpgrader")) {
            infoStream.message("IndexUpgrader", "Deleting old .geo segment files of index directory '" + dir + "'");
        }
        final IndexWriter w2 = new IndexWriter(dir, iwc);
        w2.close();
    }
  
}
