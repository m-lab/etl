
  Creates the complete set of public views for a given version.
  Also creates internal views that the public views are built on.
  This should generally be run from a travis deployment, and the
  arguments should be derived from the deployment tag.
  The following standardSQL views are created in the public dataset:
     ndt_all​ - all (lightly filtered) tests, excluding EB,
               blacklisted, short and very long tests.
     Separate views for download and upload NDT tests:
  ​​​     ndt_downloads
       ndt_uploads
 
  Notes:
  1. These are the public facing standard views.
  2. All views filter out EB test results and all views filter out tests where the blacklist_flags field is NULL.
  3. -f doesn't work on view creation, so we remove existing view first.
  4. dot (.) cannot be used within a table name, so SemVer is expressed with _.
 
  bq mk --view validates the view query, and fails if the query isn't valid.
  This means that each view must be created before being used in other
  view definitions.
 

# Expected evolution
 
  The create_view function creates views in arbitrary datasets, but the
  intended use is to create views in datasets that use semantic versioning,
  (_vMajor_Minor_Patch) with aliases intended for general public use.
 
  We expect these files to evolve over time, reflecting occasional changes in
  the source table schemas, more frequent changes in semantics and query
  details, and corresponding updates to version numbers.
 
  Changes to the views (with or without changes to underlying tables),
  would be introduced periodically as new PATCH levels.  Typically these
  would also be immediately promoted to alpha.  Periodically, perhaps
  every few months or so, a latest will be promoted to a new minor version
  number, and designated as the new beta.  After several weeks as beta
  this would then become the new public version.
 
  At any given point in time, we expect there will be something like:

  ```
    latest ->                internal_v3_3_4  
                             internal_v3_3_3  - previous latest 
    public -> internal_v3 == internal_v3_3_1  - the general public view
              internal_v2 == internal_v3_2_3  - previous minor version
 ```
  Prior to promoting a new public, we would promote a PATCH version to
  a new MINOR version, and make it the beta.
 ```
    beta, latest ->          internal_v3_4_1  - same as v3_3_4
                             internal_v3_3_4  - previous latest
    public -> internal_v3 == internal_v3_3_1  - the general public view
              internal_v2 == internal_v3_2_3  - previous minor version
 ```
 
  Beta may move ahead to addition patch versions if adjustments are
  needed.  Intermediate patch version will be removed to keep things tidy.
 ```
    latest ->                internal_v3_4_3
    beta   ->                internal_v3_4_2
    public -> internal_v3 == internal_v3_3_1  - the general public view
              internal_v2 == internal_v3_2_3  - previous minor version
 ```
 
  Eventually, the beta will be promoted to become the new public, and 
  intermediate version will be removed:
 ```
    latest ->                internal_v3_4_3
    public -> internal_v4 == internal_v3_4_2  - the general public view
              internal_v3 == internal_v3_3_1  - previous stable minor version
              internal_v2 == internal_v3_2_3  - previous minor version
 ```
 
 
  A Pull Request changing this directory will generally update this file,
  and one or more of the .sql files.  The PR will not result in a new
  deployment until a git TAG is created with the appropriate format.
 
  TAGS:
    view-v3.3.4        will generate a new v3_3_4 version, and update
                       latest to point to it
    view-beta-v3.4.1   will point beta at v3_4_1.  If v3_4_1 does not
                       already exist, it will first be created.
    view-public-v3.4.1 will create v3_4 pointing to v3_4_1, and will
                       point public and beta to the new minor view.
 
  Alternatively:
    The tags can also double as the versions and datasets.
    vM.m.p will create a new patch version, and point latest at it.
       This should generally only be done on the latest commit.
    vM.m will create a new minor version, and point beta at it.  Which
       patch version it corresponds to will only be clear from the tags.
       To move beta to a version that is not latest, tag the appropriate
       earlier commit.
    public-vM.m will cause public to point to vM_m, which must already exist.
 
  Each view should include, in its description, the git commit that created
  it.

# Scenarios
 
  Adding fields to the underlying table schema:
    Update the schema
    Update the SQL for views that should incorporate the new fields.
       (once we have all data in a single table this may not be needed)
    Update documentation
    Test the script in sandbox
    Tag the script with new minor version number, triggering deployment.
       * The new deployment should deploy to the same major version number!

