package de.jlo.datamodel;

import java.util.EventListener;

public interface DatamodelListener extends EventListener {
	
	public void eventHappend(DatamodelEvent event);
	
}
