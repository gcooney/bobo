package com.browseengine.bobo.service.impl;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;

public class BoboIndexReaderDecorator implements IndexReaderDecorator<BoboIndexReader> {
	private final List<FacetHandler<?>> _facetHandlers;
	private static final Logger log = Logger.getLogger(BoboIndexReaderDecorator.class);
	
	private final ClassLoader _classLoader;
	public BoboIndexReaderDecorator(List<FacetHandler<?>> facetHandlers)
	{
	  _facetHandlers = facetHandlers;
		_classLoader = Thread.currentThread().getContextClassLoader();
	}
	
	public BoboIndexReaderDecorator()
	{
		this(null);
	}
	
	public BoboIndexReader decorate(ZoieIndexReader<BoboIndexReader> zoieReader) throws IOException {
	    if (zoieReader != null)
	    {
    		Thread.currentThread().setContextClassLoader(_classLoader);
    		if (_facetHandlers!=null)
    		{
    		  return BoboIndexReader.getInstanceAsSubReader(zoieReader, _facetHandlers);
    		}
    		else
    		{
    		  return BoboIndexReader.getInstanceAsSubReader(zoieReader);
    		}
	    }
	    else
	    {
	      return null;
	    }
	}

	public BoboIndexReader redecorate(BoboIndexReader reader, ZoieIndexReader<BoboIndexReader> newReader)
			throws IOException {
		reader.rewrap(newReader);
		return reader;
	}
}
