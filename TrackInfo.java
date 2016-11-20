

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.object.HObject;
import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.h5.*;
import java.util.Vector;
import java.util.Arrays;


/**
 * Class containing methods to open a HDF5 song file and access its content.
 */
public class TrackInfo
{
	private H5File h5 = null;
	
	public TrackInfo(String h5_FileName)
	{
		h5 = new H5File(h5_FileName,H5File.READ);
	}
	
	public H5File getH5FileHandle()
	{
		return h5;
	}

    private void hdf5_close(H5File h5) 
    {
		try {
		    h5.close();
		} catch (HDF5Exception ex) {
		    System.out.println("Could not close HDF5 file?");
		    ex.printStackTrace();
		}
    }
 
    private int get_num_songs(H5File h5)
    {
		H5CompoundDS metadata;
		try {
		    metadata = (H5CompoundDS) h5.get("/metadata/songs");
		    metadata.init();
		    return metadata.getHeight();
		} catch (Exception e) {
		    e.printStackTrace();
		    return -1;
		}
    }

    public double get_artist_familiarity(H5File h5) throws Exception { return get_artist_familiarity(h5, 0); }
    private double get_artist_familiarity(H5File h5, int songidx) throws Exception
    {  
	return get_member_double(h5,songidx,"/metadata/songs","artist_familiarity");	
    }

    public double get_artist_hotttnesss(H5File h5) throws Exception { return get_artist_hotttnesss(h5, 0); }
    private double get_artist_hotttnesss(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/metadata/songs","artist_hotttnesss");
    }

    public String get_artist_id(H5File h5) throws Exception { return get_artist_id(h5, 0); }
    private String get_artist_id(H5File h5, int songidx) throws Exception
    {    
	return get_member_string(h5,songidx,"/metadata/songs","artist_id");
    }

    public String get_artist_mbid(H5File h5) throws Exception { return get_artist_mbid(h5, 0); }
    private String get_artist_mbid(H5File h5, int songidx) throws Exception
    {    
	return get_member_string(h5,songidx,"/metadata/songs","artist_mbid");
    }

    public int get_artist_playmeid(H5File h5) throws Exception { return get_artist_playmeid(h5, 0); }
    private int get_artist_playmeid(H5File h5, int songidx) throws Exception
    {    
    	return get_member_int(h5,songidx,"/metadata/songs","artist_playmeid");
    }

    public int get_artist_7digitalid(H5File h5) throws Exception { return get_artist_7digitalid(h5, 0); }
    private int get_artist_7digitalid(H5File h5, int songidx) throws Exception
    {    
    	return get_member_int(h5,songidx,"/metadata/songs","artist_7digitalid");
    }

    public double get_artist_latitude(H5File h5) throws Exception { return get_artist_latitude(h5, 0); }
    private double get_artist_latitude(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/metadata/songs","artist_latitude");
    }

    public double get_artist_longitude(H5File h5) throws Exception { return get_artist_longitude(h5, 0); }
    private double get_artist_longitude(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/metadata/songs","artist_longitude");
    }

    public String get_artist_location(H5File h5) throws Exception { return get_artist_location(h5, 0); }
    private String get_artist_location(H5File h5, int songidx) throws Exception
    {    
	return get_member_string(h5,songidx,"/metadata/songs","artist_location");
    }

    public String get_artist_name(H5File h5) throws Exception { return get_artist_name(h5, 0); }
    private String get_artist_name(H5File h5, int songidx) throws Exception
    {  
	return get_member_string(h5,songidx,"/metadata/songs","artist_name");
    }

    public String get_release(H5File h5) throws Exception { return get_release(h5, 0); }
    private String get_release(H5File h5, int songidx) throws Exception
    {  
	return get_member_string(h5,songidx,"/metadata/songs","release");
    }

    public int get_release_7digitalid(H5File h5) throws Exception { return get_release_7digitalid(h5, 0); }
    private int get_release_7digitalid(H5File h5, int songidx) throws Exception
    {    
	return get_member_int(h5,songidx,"/metadata/songs","release_7digitalid");
    }

    private String get_song_id(H5File h5) throws Exception { return get_song_id(h5, 0); }
    private String get_song_id(H5File h5, int songidx) throws Exception
    {    
	return get_member_string(h5,songidx,"/metadata/songs","song_id");
    }

    public double get_song_hotttnesss(H5File h5) throws Exception { return get_song_hotttnesss(h5, 0); }
    private double get_song_hotttnesss(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/metadata/songs","song_hotttnesss");
    }

    public String get_title(H5File h5) throws Exception { return get_title(h5, 0); }
    private String get_title(H5File h5, int songidx) throws Exception
    {    
	return get_member_string(h5,songidx,"/metadata/songs","title");
    }

    public int get_track_7digitalid(H5File h5) throws Exception { return get_track_7digitalid(h5, 0); }
    private int get_track_7digitalid(H5File h5, int songidx) throws Exception
    {    
	return get_member_int(h5,songidx,"/metadata/songs","track_7digitalid");
    }

    private String[] get_similar_artists(H5File h5) throws Exception { return get_similar_artists(h5, 0); }
    private String[] get_similar_artists(H5File h5, int songidx) throws Exception
    {    
	return get_array_string(h5, songidx, "metadata", "similar_artists");
    }

    private String[] get_artist_terms(H5File h5) throws Exception { return get_artist_terms(h5, 0); }
    private String[] get_artist_terms(H5File h5, int songidx) throws Exception
    {    
	return get_array_string(h5, songidx, "metadata", "artist_terms");
    }

    private double[] get_artist_terms_freq(H5File h5) throws Exception { return get_artist_terms_freq(h5, 0); }
    private double[] get_artist_terms_freq(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "metadata", "artist_terms_freq",1,"idx_artist_terms");
    }

    private double[] get_artist_terms_weight(H5File h5) throws Exception { return get_artist_terms_weight(h5, 0); }
    private double[] get_artist_terms_weight(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "metadata", "artist_terms_weight",1,"idx_artist_terms");
    }

    private double get_analysis_sample_rate(H5File h5) throws Exception { return get_analysis_sample_rate(h5, 0); }
    private double get_analysis_sample_rate(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","analysis_sample_rate");
    }

    private String get_audio_md5(H5File h5) throws Exception { return get_audio_md5(h5, 0); }
    private String get_audio_md5(H5File h5, int songidx) throws Exception
    {    
	return get_member_string(h5,songidx,"/analysis/songs","audio_md5");
    } 

    private double get_danceability(H5File h5) throws Exception { return get_danceability(h5, 0); }
    private double get_danceability(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","danceability");
    } 

    private double get_duration(H5File h5) throws Exception { return get_duration(h5, 0); }
    private double get_duration(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","duration");
    }

    private double get_end_of_fade_in(H5File h5) throws Exception { return get_end_of_fade_in(h5, 0); }
    private double get_end_of_fade_in(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","end_of_fade_in");
    }

    private double get_energy(H5File h5) throws Exception { return get_energy(h5, 0); }
    private double get_energy(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","energy");
    } 

    private int get_key(H5File h5) throws Exception { return get_key(h5, 0); }
    private int get_key(H5File h5, int songidx) throws Exception
    {    
	return get_member_int(h5,songidx,"/analysis/songs","key");
    }

    private double get_key_confidence(H5File h5) throws Exception { return get_key_confidence(h5, 0); }
    private double get_key_confidence(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","key_confidence");
    }
 
    private double get_loudness(H5File h5) throws Exception { return get_loudness(h5, 0); }
    private double get_loudness(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","loudness");
    }

    private int get_mode(H5File h5) throws Exception { return get_mode(h5, 0); }
    private int get_mode(H5File h5, int songidx) throws Exception
    {    
	return get_member_int(h5,songidx,"/analysis/songs","mode");
    }

    private double get_mode_confidence(H5File h5) throws Exception { return get_mode_confidence(h5, 0); }
    private double get_mode_confidence(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","mode_confidence");
    }

    private double get_start_of_fade_out(H5File h5) throws Exception { return get_start_of_fade_out(h5, 0); }
    private double get_start_of_fade_out(H5File h5, int songidx) throws Exception
    {   
	return get_member_double(h5,songidx,"/analysis/songs","start_of_fade_out");
    }

    private double get_tempo(H5File h5) throws Exception { return get_tempo(h5, 0); }
    private double get_tempo(H5File h5, int songidx) throws Exception
    {
	return get_member_double(h5,songidx,"/analysis/songs","tempo");
    }

    private int get_time_signature(H5File h5) throws Exception { return get_time_signature(h5, 0); }
    private int get_time_signature(H5File h5, int songidx) throws Exception
    {   
	return get_member_int(h5,songidx,"/analysis/songs","time_signature");
    }

    private double get_time_signature_confidence(H5File h5) throws Exception { return get_time_signature_confidence(h5, 0); }
    private double get_time_signature_confidence(H5File h5, int songidx) throws Exception
    {    
	return get_member_double(h5,songidx,"/analysis/songs","time_signature_confidence");
    }

    private String get_track_id(H5File h5) throws Exception { return get_track_id(h5, 0); }
    private String get_track_id(H5File h5, int songidx) throws Exception
    {   
	return get_member_string(h5,songidx,"/analysis/songs","track_id");
    }

    private double[] get_segments_start(H5File h5) throws Exception { return get_segments_start(h5, 0); }
    private double[] get_segments_start(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "segments_start", 1);
    }

    private double[] get_segments_confidence(H5File h5) throws Exception { return get_segments_confidence(h5, 0); }
    private double[] get_segments_confidence(H5File h5, int songidx) throws Exception
    {   
	return get_array_double(h5, songidx, "analysis", "segments_confidence", 1);
    }

    private double[] get_segments_pitches(H5File h5) throws Exception { return get_segments_pitches(h5, 0); }
    private double[] get_segments_pitches(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "segments_pitches", 2);
    }

    private double[] get_segments_timbre(H5File h5) throws Exception { return get_segments_timbre(h5, 0); }
    private double[] get_segments_timbre(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "segments_timbre", 2);
    }

    private double[] get_segments_loudness_max(H5File h5) throws Exception { return get_segments_loudness_max(h5, 0); }
    private double[] get_segments_loudness_max(H5File h5, int songidx) throws Exception
    {   
	return get_array_double(h5, songidx, "analysis", "segments_loudness_max", 1);
    }

    private double[] get_segments_loudness_max_time(H5File h5) throws Exception { return get_segments_loudness_max_time(h5, 0); }
    private double[] get_segments_loudness_max_time(H5File h5, int songidx) throws Exception
    {   
	return get_array_double(h5, songidx, "analysis", "segments_loudness_max_time", 1);
    }

    private double[] get_segments_loudness_start(H5File h5) throws Exception { return get_segments_loudness_start(h5, 0); }
    private double[] get_segments_loudness_start(H5File h5, int songidx) throws Exception
    {   
	return get_array_double(h5, songidx, "analysis", "segments_loudness_start", 1);
    }

    private double[] get_sections_start(H5File h5) throws Exception { return get_sections_start(h5, 0); }
    private double[] get_sections_start(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "sections_start", 1);
    }

    private double[] get_sections_confidence(H5File h5) throws Exception { return get_sections_confidence(h5, 0); }
    private double[] get_sections_confidence(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "sections_confidence", 1);
    }

    private double[] get_beats_start(H5File h5) throws Exception { return get_beats_start(h5, 0); }
    private double[] get_beats_start(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "beats_start", 1);
    }

    private double[] get_beats_confidence(H5File h5) throws Exception { return get_beats_confidence(h5, 0); }
    private double[] get_beats_confidence(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "beats_confidence", 1);
    }

    private double[] get_bars_start(H5File h5) throws Exception { return get_bars_start(h5, 0); }
    private double[] get_bars_start(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "bars_start", 1);
    }

    private double[] get_bars_confidence(H5File h5) throws Exception { return get_bars_confidence(h5, 0); }
    private double[] get_bars_confidence(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "bars_confidence", 1);
    }

    private double[] get_tatums_start(H5File h5) throws Exception { return get_tatums_start(h5, 0); }
    private double[] get_tatums_start(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "tatums_start", 1);
    }

    private double[] get_tatums_confidence(H5File h5) throws Exception { return get_tatums_confidence(h5, 0); }
    private double[] get_tatums_confidence(H5File h5, int songidx) throws Exception
    {    
	return get_array_double(h5, songidx, "analysis", "tatums_confidence", 1);
    }

    private int get_year(H5File h5) throws Exception { return get_year(h5, 0); }
    private int get_year(H5File h5, int songidx) throws Exception
    {    
	return get_member_int(h5,songidx,"/musicbrainz/songs","year");
    }

    private String[] get_artist_mbtags(H5File h5) throws Exception { return get_artist_mbtags(h5, 0); }
    private String[] get_artist_mbtags(H5File h5, int songidx) throws Exception
    {    
	return get_array_string(h5, songidx, "musicbrainz", "artist_mbtags");
    }

    private int[] get_artist_mbtags_count(H5File h5) throws Exception { return get_artist_mbtags_count(h5, 0); }
    private int[] get_artist_mbtags_count(H5File h5, int songidx) throws Exception
    {    
	return get_array_int(h5, songidx, "musicbrainz", "artist_mbtags_count","idx_artist_mbtags");
    }

    /********************************** UTILITY FUNCTIONS ************************************/

    /**
     * Slow utility function.
     */
    private int find(String[] tab, String key)
    {
	for (int k = 0; k < tab.length; k++)
	    if (tab[k].equals(key)) return k;
	return -1;
    }

    private int get_member_int(H5File h5, int songidx, String table, String member) throws Exception
    {    
	H5CompoundDS analysis = (H5CompoundDS) h5.get(table);
	analysis.init();
	int wantedMember = find( analysis.getMemberNames() , member);
	assert(wantedMember >= 0);		
	Vector alldata = (Vector) analysis.getData();
	int[] col = (int[]) alldata.get(wantedMember);
	return col[songidx];
    }

    private double get_member_double(H5File h5, int songidx, String table, String member) throws Exception
    {    
	H5CompoundDS analysis = (H5CompoundDS) h5.get(table);
	analysis.init();
	int wantedMember = find( analysis.getMemberNames() , member);
	assert(wantedMember >= 0);		
	Vector alldata = (Vector) analysis.getData();
	double[] col = (double[]) alldata.get(wantedMember);
	return col[songidx];
    }

    private String get_member_string(H5File h5, int songidx, String table, String member) throws Exception
    {    
	H5CompoundDS analysis = (H5CompoundDS) h5.get(table);
	analysis.init();
	int wantedMember = find( analysis.getMemberNames() , member);
	assert(wantedMember >= 0);		
	Vector alldata = (Vector) analysis.getData();
	String[] col = (String[]) alldata.get(wantedMember);
	return col[songidx];
    }

    private double[] get_array_double(H5File h5, int songidx, String group, String arrayname, int ndims) throws Exception   
    {
	return get_array_double(h5,songidx,group,arrayname,ndims,"");
    }
    private double[] get_array_double(H5File h5, int songidx, String group, String arrayname, int ndims, String idxname) throws Exception
    {    
	// index
	H5CompoundDS analysis = (H5CompoundDS) h5.get(group + "/songs");
	analysis.init();
	if (idxname.equals("")) idxname = "idx_"+arrayname;
	int wantedMember = find( analysis.getMemberNames() , idxname);
	assert(wantedMember >= 0);		
	Vector alldata = (Vector) analysis.getData();
	int[] col = (int[]) alldata.get(wantedMember);
	int pos1 = col[songidx];
	// data
	H5ScalarDS array = (H5ScalarDS) h5.get("/"+group+"/"+arrayname);
	if (ndims == 1)
	    {
		double[] data = (double[]) array.getData();
		int pos2 = data.length;
		if (songidx + 1 < col.length) pos2 = col[songidx+1];
		// copy
		double[] res = new double[pos2-pos1];
		for (int k = 0; k < res.length; k++)
		    res[k] = data[pos1+k];
		return res;
	    }
	else if (ndims == 2) // multiply by 12
	    {
		pos1 *= 12;
		double[] data = (double[]) array.getData();
		int pos2 = data.length;
		if (songidx + 1 < col.length) pos2 = col[songidx+1] * 12;
		// copy
		double[] res = new double[pos2-pos1];
		for (int k = 0; k < res.length; k++)
		    res[k] = data[pos1+k];
		return res;
	    }
	// more than 2 dims?
	return null;
    }

    private int[] get_array_int(H5File h5, int songidx, String group, String arrayname) throws Exception
    {
	return get_array_int(h5, songidx, group, arrayname, "");
    }
    private int[] get_array_int(H5File h5, int songidx, String group, String arrayname, String idxname) throws Exception
    {    
	// index
	H5CompoundDS analysis = (H5CompoundDS) h5.get(group + "/songs");
	analysis.init();
	if (idxname.equals("")) idxname = "idx_"+arrayname;
	int wantedMember = find( analysis.getMemberNames() , idxname);
	assert(wantedMember >= 0);		
	Vector alldata = (Vector) analysis.getData();
	int[] col = (int[]) alldata.get(wantedMember);
	int pos1 = col[songidx];
	// data
	H5ScalarDS array = (H5ScalarDS) h5.get("/"+group+"/"+arrayname);
	int[] data = (int[]) array.getData();
	int pos2 = data.length;
	if (songidx + 1 < col.length) pos2 = col[songidx+1];
	// copy
	int[] res = new int[pos2-pos1];
	for (int k = 0; k < res.length; k++)
	    res[k] = data[pos1+k];
	return res;
    }

    private String[] get_array_string(H5File h5, int songidx, String group, String arrayname) throws Exception
    {    
	// index
	H5CompoundDS analysis = (H5CompoundDS) h5.get(group + "/songs");
	analysis.init();
	int wantedMember = find( analysis.getMemberNames() , "idx_"+arrayname);
	assert(wantedMember >= 0);		
	Vector alldata = (Vector) analysis.getData();
	int[] col = (int[]) alldata.get(wantedMember);
	int pos1 = col[songidx];
	// data
	H5ScalarDS array = (H5ScalarDS) h5.get("/"+group+"/"+arrayname);
	String[] data = (String[]) array.getData();
	int pos2 = data.length;
	if (songidx + 1 < col.length) pos2 = col[songidx+1];
	// copy
	String[] res = new String[pos2-pos1];
	for (int k = 0; k < res.length; k++)
	    res[k] = data[pos1+k];
	return res;
    }
    //Getters
    
    
    
}