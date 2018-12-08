import java.util.*;

/**
 * This class initializes variables and sites. It also holds the list of variables and sites.
 *
 * @author Hyung Jin Cho
 * @version 12/07/2018
 * @see Site
 */
class SiteManager {
  private static final int SITE = 10;
  private static final int VARIABLE = 20;

  private List<Site> sites;
  private final List<Variable> variables = new ArrayList<>();

  /**
   * Constructor which initializes sites and variables
   */
  SiteManager() {
    initializesSites();
  }

  /**
   * Returns a list of all created sites
   *
   * @return The list of all created site objects. Returns an empty list if no sites have been created.
   */
  List<Site> getSites() {
    return new ArrayList<>(sites);
  }

  /**
   * Initializes each site with corresponding variables.
   */
  private void initializesSites() {
    sites = new ArrayList<>();
    String variableId;
    Variable variable;
    Site site;

    // Create sites
    for (int i = 1; i <= SITE; i++) {
      site = new Site(i);
      sites.add(site);
    }

    // Add variables into each sites
    for (int i = 1; i <= VARIABLE; i++) {
      variableId = "x" + i;
      variable = new Variable(variableId);
      variables.add(variable);

      if (i % 2 == 1) {
        int siteId = 1 + i % SITE;
        sites.get(siteId - 1).addVariableToSite(variables.get(i - 1));
      }
      for (Site s : sites) {
        s.addVariableToSite(deepCopyVariable(variables.get(i - 1)));
      }
    }
  }

  /**
   * Makes deep copy of variable
   *
   * @param variable The variable object
   * @return The new copied variable object
   * @throws NullPointerException if variableId is null
   */
  private Variable deepCopyVariable(Variable variable) {
    return new Variable(Objects.requireNonNull(variable, "variable must not be null").getVariableId());
  }


  /**
   * Gets site with given variable id. If variable id is even, it returns all available site, otherwise returns
   * specific site which is (id +1 % 10).
   *
   * @param variableId The id of variable object
   * @return The site
   */
  Site getSite(String variableId) {
    int id = Integer.parseInt(variableId.substring(1));
    if (id % 2 != 1) {
      for (Site site : getSites()) {
        if (site.getStatus() == Site.Status.UP && site.getVariable(variableId).getIsReadable()) {
          return site;
        }
      }
      System.out.println("@Comment: There is no available site for accessing " + variableId);
      return null;
    } else {
      int siteId = id + 1 % 10;
      Site site = getSites().get(siteId - 1);
      if (site.getStatus() == Site.Status.DOWN) {
        return null;
      }
      return site;
    }
  }
}
