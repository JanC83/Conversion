package conversion;

public class ConvertMain {

	public static void main(String[] args) {
		try{
     Convert convert = new Convert();
     convert.convPlaats();
     convert.convGebruiker();
     convert.convKlant();
     convert.convOpdracht();
     convert.close();
	}
		catch (Exception e){
			e.printStackTrace();
		}
 }
}
