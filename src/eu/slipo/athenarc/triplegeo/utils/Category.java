/*
 * @(#) Category.java 	 version 1.3   14/11/2017
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
 * A category item used for classification of entities.
 *
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 7/11/2017
 * Last modified by: Kostas Patroumpas, 14/11/2017
 */

public class Category {
	private String URI;             //URI assigned to this category during transformation
	private String id;
	private String name;
	private String parent;
	
	
//	public List<Classification> children = new ArrayList<Classification>();
	
	public Category(String URI, String id, String name, String parent) {
		this.URI = URI.isEmpty() ? null : URI;
		this.id = id.isEmpty() ? null : id;
		this.name = name.isEmpty() ? null : name;
		this.parent = parent.isEmpty() ? null : parent;
	}

	public String getURI() {
		return URI;
	}

	public void setURI(String uri) {
		this.URI = uri;                //Update URI of a category
	}
	
	public String getId() {
		return id;
	}
	
	public String getParent() {
		return parent;
	}
	
	public String getName() {
		return name;
	}

	public boolean hasURI() {
		return URI != null;
	}
	
	public boolean hasId() {
		return id != null;
	}
	
	public boolean hasParent() {
		return parent != null;
	}
	
	public boolean hasName() {
		return name != null;
	}
	
	public String typeContents() {
		return id + " " + name + " " + parent;
	}
}