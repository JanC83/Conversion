package conversion;

import java.sql.*;

public class Convert {
	public static final String DBBRON = "Betis";
	public static final String DBDOEL = "C:/Ontwikkelpracticum/Opdracht 5/Opdracht_04.fdb";
	public static final String DOELUSER = "sysdba";
	public static final String DOELPASSWORD = "masterkey";
	public static final String DOELDRIVERNAME = "org.firebirdsql.jdbc.FBDriver";
	public static final String DOELURL = "jdbc:firebirdsql:localhost:"+DBDOEL;
	public static final String BRONURL = "jdbc:mysql://localhost/"+DBBRON;
	public static final String BRONUSER = "root";
	public static final String BRONPASSWORD = "Accesdenied";
	public static final String BRONDRIVERNAME = "com.mysql.jdbc.Driver";
	public static final String DOELQUERY = "";
	private Connection cbron, cdoel = null;
	private PreparedStatement bronPs, doelPs = null;
	
    public Convert()throws Exception {
    	Class.forName(DOELDRIVERNAME);
    	Class.forName(BRONDRIVERNAME);
    	System.out.println("JDBC-drivers zijn geladen");
    	cbron = DriverManager.getConnection(BRONURL, BRONUSER, BRONPASSWORD);
    	cdoel = DriverManager.getConnection(DOELURL, DOELUSER, DOELPASSWORD);
    	System.out.println("Connections OK");
    }
    
    public void convGebruiker()throws Exception {
    	final String BRONQUERYGEBRUIKER = "select naam from Gebruiker";
    	final String DELQUERY = "delete from Medewerker";
    	final String INSQUERY = "insert into Medewerker values(?)";
    	bronPs = cbron.prepareStatement(BRONQUERYGEBRUIKER);
    	doelPs = cdoel.prepareStatement(DELQUERY);
    	doelPs.executeUpdate();
    	System.out.println("Tabel Medewerker leeggemaakt");
    	doelPs = cdoel.prepareStatement(INSQUERY);
    	ResultSet rs = bronPs.executeQuery();
    	while (rs.next()){
    		String naam = rs.getString("naam");
    		System.out.println(naam);
    		doelPs.setString(1, naam);
    		doelPs.executeUpdate();
    	}
    	System.out.println("Tabel Medewerker is geconverteerd");
    	rs.close();
    }
    
    public void convPlaats()throws Exception {
    	final String BRONQUERYGEBRUIKER = "select naam from Plaats";
    	final String DELQUERY = "delete from Plaats";
    	final String INSQUERY = "insert into Plaats values(?)";
    	bronPs = cbron.prepareStatement(BRONQUERYGEBRUIKER);
    	doelPs = cdoel.prepareStatement(DELQUERY);
    	doelPs.executeUpdate();
    	System.out.println("Tabel Plaats leeggemaakt");
    	doelPs = cdoel.prepareStatement(INSQUERY);
    	ResultSet rs = bronPs.executeQuery();
    	while (rs.next()){
    		String naam = rs.getString("naam");
    		doelPs.setString(1, naam);
    		doelPs.executeUpdate();
    	}
    	System.out.println("Tabel Plaats is geconverteerd");
    	rs.close();
    }
    
    public void convKlant()throws Exception {
    	final String BRONQUERYGEBRUIKER = "select * from Klant";
    	final String DELQUERY = "delete from Klant";
    	final String INSQUERY = "insert into Klant(nr, naam, straat, huisnr, postcode, plaats, geblokkeerd) values(?,?,?,?,?,?,?)";
    	bronPs = cbron.prepareStatement(BRONQUERYGEBRUIKER);
    	doelPs = cdoel.prepareStatement(DELQUERY);
    	doelPs.executeUpdate();
    	System.out.println("Tabel Klant leeggemaakt");
    	doelPs = cdoel.prepareStatement(INSQUERY);
    	ResultSet rs = bronPs.executeQuery();
    	int i = 0;
    	while (rs.next()){
    		int nr = rs.getInt("nr");
            String naam = rs.getString("naam");
            String contactpersoon = rs.getString("CP");
            String straat = rs.getString("straat");
            String postcode = formatPostcode(rs.getString("PC"));
            String huisnr = rs.getString("huisnr");
            String plaats = rs.getString("plaats");
            String telefoonnr = rs.getString("tel");
            String notitie = rs.getString("notitie");
            String blok = rs.getString("blok");
            doelPs.setInt(1, nr);
            doelPs.setString(2, naam);
            doelPs.setString(3, straat);
            doelPs.setString(4, huisnr);
            doelPs.setString(5, postcode);
            doelPs.setString(6, plaats);
            doelPs.setString(7, blok);
            doelPs.executeUpdate();
            vulKlantNfa(nr, notitie, telefoonnr);
            if (contactpersoon !=  null){
            splitsContactpersoon(nr, contactpersoon);
            }
            if(i%100==0){
            System.out.println(i);
            }
            i++;
    	}
    	System.out.println("Tabel Klant is geconverteerd");
    	rs.close();
    }
    
    public void convOpdracht()throws Exception {
    	wisFoutieveRecordsOpdracht();
    	final String BRONQUERYGEBRUIKER = "select * from Opdracht";
    	final String DELQUERY = "delete from Opdracht";
    	final String INSQUERY = "insert into Opdracht(nr, datum_opdracht, klant, aantal_colli, gewicht, naar_straat, naar_huisnr, naar_postcode, naar_plaats, datum_planning, datum_transport, bon_ontvangen_, medewerker, bedrag) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    	bronPs = cbron.prepareStatement(BRONQUERYGEBRUIKER);
    	doelPs = cdoel.prepareStatement(DELQUERY);
    	doelPs.executeUpdate();
    	System.out.println("Tabel Opdracht leeggemaakt");
    	doelPs = cdoel.prepareStatement(INSQUERY);
    	ResultSet rs = bronPs.executeQuery();
    	int i = 0;
    	while (rs.next()){
    		int nr = rs.getInt("nr");
    		Date datumOpdracht = rs.getDate("dopdr");
    		int klant = rs.getInt("klantnr");
    		int aantalColli = rs.getInt("colli");
    		double gewicht = rs.getDouble("kg");
    		String naarStraat = rs.getString("straat");
    		String huisnr = rs.getString("huisnr");
    		String postcode = formatPostcode(rs.getString("pc"));
    		String plaats = rs.getString("plaats");
    		Date datumPlanning = rs.getDate("dplan");
    		Date datumTransport = rs.getDate("dtrans");
    		String notitie = rs.getString("notitie");
    		String bonOntvangen = rs.getString("bonbin");
    		String medewerker = rs.getString("mdw");
    		Double bedrag = rs.getDouble("bedrag");
    		doelPs.setInt(1, nr);
    		doelPs.setDate(2, datumOpdracht);
    		doelPs.setInt(3, klant);
    		if (aantalColli==0){
    			doelPs.setInt(4, 0);
    		}
    		else {
    			doelPs.setInt(4, aantalColli);
    		}
    		if (gewicht==0.00){
    			doelPs.setDouble(5, 0.00);
    		}
    		else {
    			doelPs.setDouble(5, gewicht);
    		}
    		doelPs.setString(6, naarStraat);
    		doelPs.setString(7, huisnr);
    		doelPs.setString(8, postcode);
    		doelPs.setString(9, plaats);
    		if(datumPlanning==null){
    			doelPs.setDate(10, new Date(0L));
    		}
    		else{
    			doelPs.setDate(10, datumPlanning);
    		}
    		if(datumTransport==null){
    			doelPs.setDate(11, new Date(0L));
    		}
    		else{
    			doelPs.setDate(11, datumTransport);
    		}
    		doelPs.setString(12, bonOntvangen);
    		if(medewerker==null){
    			doelPs.setString(13, "medewerker onbekend");
    		}
    		else{
    			doelPs.setString(13, medewerker);
    		}
    		doelPs.setDouble(14, bedrag);
    		doelPs.executeUpdate();
    		if(i%100==0){
    		System.out.println(i);
    		}
    		i++;
    	}
    	System.out.println("Tabel Opdracht is geconverteerd");
    	rs.close();
    }
    
    public void close() throws Exception{
    	if(cbron!=null){
    		cbron.close();
    	}
    	if(cdoel!=null){
    		cdoel.close();
    	}
    	System.out.println("Connecties gesloten");
    }
    
    private void vulKlantNfa(int klant, String notitie, String telefoonnr) throws Exception{
    		PreparedStatement nfa = cdoel.prepareStatement("insert into KLANT_NFA (klant, nfa_type, NUMMER_ADRES) values (?,?,?)");
    	if (notitie != null && isEmail(notitie)){
    		nfa.setInt(1, klant);
    		nfa.setString(2, "e-mail");
    		nfa.setString(3, notitie);
    		nfa.executeUpdate();
    	}
    	if (telefoonnr != null && telefoonnr.matches(".*\\d+.*")){
    	   nfa.setInt(1, klant);
		   nfa.setString(2, "telefoon");
		   nfa.setString(3, telefoonnr);
		   nfa.executeUpdate(); 
    	}
    }
    
    private String vindGeslacht(String cp){
    	String s = cp.toLowerCase();
    	if (s.indexOf("de heer")!=-1){
    		return "M";
    	}
    	if (s.indexOf("dhr.")!=-1){
    		return "M";
    	}
    	if (s.indexOf("dhr")!=-1){
    		return "M";
    	}
    	if (s.indexOf("mevrouw")!=-1){
    		return "M";
    	}
    	if (s.indexOf("mw.")!=-1){
    		return "M";
    	}
    	if (s.indexOf("mw")!=-1){
    		return "M";
    	}
    	else {
    		return "O";
    	}
    	}
    
    private String vindNaam(String cp){
    	String s = cp.toLowerCase();
    	if (s.indexOf("de heer")!=-1){
    		return cp.substring(7).trim();
    	}
    	if (s.indexOf("dhr.")!=-1){
    		return cp.substring(4).trim();
    	}
    	if (s.indexOf("dhr")!=-1){
    		return cp.substring(3).trim();
    	}
    	if (s.indexOf("mevrouw")!=-1){
    		return cp.substring(7).trim();
    	}
    	if (s.indexOf("mw.")!=-1){
    		return cp.substring(3).trim();
    	}
    	if (s.indexOf("mw")!=-1){
    		return cp.substring(2).trim();
    	}
    	else{
    		return cp;
    	}
    }
    
    private void splitsContactpersoon(int klant, String cp) throws Exception{
    	PreparedStatement splits = cdoel.prepareStatement("insert into Contactpersoon (klant, geslacht, achternaam) values (?,?,?)");
    	String geslacht = vindGeslacht(cp);
    	String naam = vindNaam(cp);
    	splits.setInt(1, klant);
    	splits.setString(2,geslacht);
    	splits.setString(3, naam);
    	splits.executeUpdate();
    }
    
    private String formatPostcode(String pc){
    	String cijfers = pc.substring(0, 4);
    	String letters = pc.substring(4);
    	return cijfers + " " + letters;
    	
    }
    
    private boolean isEmail (String s){
    	boolean b = false;
    	if (s!=null){
    		int i;
    		for(i = 0; i<s.length(); i++){
    			if (s.charAt(i) == '@'){
    				b = true;
    			}
    		}		
    	}
    	return b;
    }
    
    private void wisFoutieveRecordsOpdracht() throws Exception{
    	PreparedStatement wis = cbron.prepareStatement("delete from Opdracht "
    			                                       + "where klantnr not in "
    			                                       + "(select nr from Klant)");
    	wis.executeUpdate();
    }
 
}
