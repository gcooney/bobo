package com.browseengine.bobo.geosearch.store;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.browseengine.bobo.geosearch.index.impl.GeoIndexUpgrader;

/**
 * DirectoryWrapper that blocks deletion of .geo files.  This is needed to prevent lucene 4x from
 * deleting geo files written against lucene 3x.  
 * 
 * This blocks ALL cleanup of .geo files.  It is strongly recommended that users upgrade their
 * index to lucene4x using {@link GeoIndexUpgrader}.  Once the upgrade is complete, this directory
 * wrapper is no longer necessary
 * @author gcooney
 *
 */
public class BlockGeoDeleteDirectory extends Directory {

    private Directory primaryDir;

    public BlockGeoDeleteDirectory(Directory primaryDir) {
        this.primaryDir = primaryDir;
    }
    
    @Override
    public String[] listAll() throws IOException {
        return primaryDir.listAll();
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return primaryDir.fileExists(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (!name.endsWith(".geo")) {
           primaryDir.deleteFile(name); 
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        return primaryDir.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return primaryDir.createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        primaryDir.sync(names);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return primaryDir.openInput(name, context);
    }

    @Override
    public void close() throws IOException {
        primaryDir.close();
    }
    
    @Override
    public Lock makeLock(String name) {
      return primaryDir.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        primaryDir.clearLock(name);
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        primaryDir.setLockFactory(lockFactory);
    }

    @Override
    public LockFactory getLockFactory() {
        return primaryDir.getLockFactory();
    }

    @Override
    public String getLockID() {
        return primaryDir.getLockID();
    }

    @Override
    public String toString() {
        return "BlockGeoDeleteDirectory(" + primaryDir.toString() + ")";
    }

    @Override
    public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
        primaryDir.copy(to, src, dest, context);
    }

    @Override
    public Directory.IndexInputSlicer createSlicer(final String name, final IOContext context) throws IOException {
        return primaryDir.createSlicer(name, context);
    }

}
