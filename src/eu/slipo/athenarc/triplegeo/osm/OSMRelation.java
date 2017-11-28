package eu.slipo.athenarc.triplegeo.osm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class containing information about the OSM relations.
 * 
 * @author Nikos Karagiannakis
 * Last revised: 7/9/2017 by Kostas Patroumpas
 */
public class OSMRelation implements Serializable{
    
    private static final long serialVersionUID = 1L;    
    private String id;
    private Set<Integer> classIDs;
    private final Map<String, String> memberReferences = new HashMap<>();
    private final Map<String, String> tags = new HashMap<>();
    
    public String getID(){
        return id;
    }
    
    public Map<String, String> getMemberReferences(){
        return memberReferences;
    }
    
    public Set<Integer> getClassIDs(){
        return this.classIDs;
    }
    
    public Map<String, String> getTagKeyValue(){
        return tags;
    }
    
    public void setID(String id){
        this.id = id;
    }
    
    public void setClassIDs(Set<Integer> classIDs){
        this.classIDs = classIDs;
    }    
    
    public void addMemberReference(String memberReference, String role){
        this.memberReferences.put(memberReference, role);
    }    
    
    public void setTagKeyValue(String tagKey, String tagValue){
        this.tags.put(tagKey, tagValue);
    }
}