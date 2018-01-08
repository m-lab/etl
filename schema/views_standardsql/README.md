
  Creates the complete set of release views for a given version.
  Also creates intermediate views that the release views are built on.
  This should generally be run from a travis deployment.

  Dataset names (including versioning) should be hard coded into the
  travis deployment scripts, so that the commit contains all required
  information.

  The following standardSQL views are created in the release dataset:
     ndt_all - all (lightly filtered) tests, excluding EB,
               blacklisted, short and very long tests.
     Separate views for download and upload NDT tests:
       ndt_downloads
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
  (_vMajor_Minor_Patch) with aliases intended for general release use.

  We expect these files to evolve over time, reflecting occasional changes in
  the source table schemas, more frequent changes in semantics and query
  details, and corresponding updates to version numbers.

  Changes to the views (with or without changes to underlying tables),
  would be introduced periodically as new PATCH levels.  Typically these
  would also be immediately promoted to alpha.  Periodically, perhaps
  every few months or so, a latest will be promoted to a new minor version
  number, and designated as the new rc.  After several weeks as rc
  this would then become the new release version.

  At any given point in time, we expect there will be something like:

  ```
    latest ->                     intermediate_v3_3_4
                                  intermediate_v3_3_3  - previous latest
    release -> intermediate_v3 == intermediate_v3_3_1  - the general release view
               intermediate_v2 == intermediate_v3_2_3  - previous minor version
 ```
  Prior to promoting a new release, we would promote a PATCH version to
  a new MINOR version, and make it the rc.
 ```
    rc, latest ->                   intermediate_v3_4_1  - release candidate
                                    intermediate_v3_3_4  - previous latest (==v3_4_1)
    release -> intermediate_v3_3 == intermediate_v3_3_1  - the general release view
               intermediate_v3_2 == intermediate_v3_2_3  - previous minor version
 ```

  'latest' may move ahead to additional patch versions if adjustments are
  needed.  Intermediate patch version will be removed to keep things tidy.
 ```
    latest ->                       intermediate_v3_4_3
    rc   ->                         intermediate_v3_4_2  - current release candidate
    release -> intermediate_v3_3 == intermediate_v3_3_1  - the general release view
               intermediate_v3_2 == intermediate_v3_2_3  - previous minor version
 ```

  Eventually, the rc will be promoted to become the new release, and
  intermediate version will be removed:
 ```
    latest ->                       intermediate_v3_4_3
    release -> intermediate_v3_4 == intermediate_v3_4_2  - the general release view
               intermediate_v3_3 == intermediate_v3_3_1  - previous stable minor version
               intermediate_v3_2 == intermediate_v3_2_3  - previous minor version
 ```


  A Pull Request changing this directory will generally update this file,
  and one or more of the .sql files.  The PR will not result in a new
  deployment until a git TAG is created with the appropriate format.

## Scenarios

### 1. To add fields to the underlying table schema:
* Update the schema
* Update the SQL for views that should incorporate the new fields.
       (once we have all data in a single table this may not be needed)
* Update the minor version number in the .travis.yml file.
* Update documentation
* Test the script in sandbox
* Tag the script with new minor version number, triggering deployment.
     * The new deployment should deploy to the same major version number!

