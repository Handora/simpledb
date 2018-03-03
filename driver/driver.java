import java.net.*;
import java.io.*;
import java.util.*;

class driver {
  public static void main(String args[]) throws Exception {
    Socket s = new Socket("127.0.0.1", 51202);
    PrintWriter dout = new PrintWriter(s.getOutputStream(), true);
    BufferedReader din = new BufferedReader(new InputStreamReader(s.getInputStream()));

    String st = String.format("select passwd from test where user='%s';\n", args[0]);
    dout.print(st);
    dout.flush();
    boolean ok = false;
    ArrayList<String> l = new ArrayList<>();

    while(true) {
      String str = din.readLine();
      if (str.contains("seconds")) {
        break;
      }

      if (ok) {
        if (str.length() == 0) {
          ok = false;
        } else {
          l.add(str);
        }
      }

      if (str.contains("--")) {
        ok = true;
      }
    }
    s.close();
    for (String ss: l) {
      System.out.println(ss);
    }
  }
}
