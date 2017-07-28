package eu.slipo.athenarc.triplegeo.osm;

import com.vividsolutions.jts.geom.Geometry;
import java.util.HashMap;
import java.util.Map;

/**
 * Class containing information about the OSM records (nodes, ways, or relations).
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 25/7/2017
 * Last modified by: Kostas Patroumpas, 28/7/2017
 */

public class OSMRecord {
    
    private String id;
	private String name;
	private String type;
    private Geometry geometry;
	private final Map<String, String> tags = new HashMap<>(); 
    
    //attribute getters
    public String getID(){
        return id;
    }

    public String getName(){
        return name;
    }
    
    public String getType(){
        return type;
    } 

    public Geometry getGeometry(){
        return this.geometry;
    }
    
    public Map<String,String> getTagKeyValue(){
        return tags;
    }      
    
    //attributes setters
    public void setID(String id){
        this.id = id;
    }
    
    public void setName(String name){
        this.name = name;
    }
    
    public void setType(String type){
        this.type = type;
    }
    
    public void setGeometry(Geometry geometry){
        this.geometry = geometry;
    }
    
    public void setTagKeyValue(String tagKey, String tagValue){
        this.tags.put(tagKey, tagValue);
    }
    
    public void setTags(Map<String, String> tags){
        this.tags.putAll(tags);
    }
}