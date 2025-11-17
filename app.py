from pymongo import MongoClient
from bson import ObjectId
import pprint

# -------------------------
# Database connection
# -------------------------
client = MongoClient("mongodb+srv://fake_user:user1@aphiwenoahcluster0.zq4hdcq.mongodb.net/?retryWrites=true&w=majority&appName=aphiwenoahcluster0")  # adjust if using Atlas
db = client["university_db"]      # replace with your chosen DB name

collection = db["students"]    # example collection
collection2 = db["professors"]
collection3 = db["courses"]

pp = pprint.PrettyPrinter(indent=2)


# -------------------------
# CRUD Function Templates
# -------------------------

###################################### CREATE #################################################


def create_document(doc, collection):
    """Insert a single document into the collection."""
    try:
        result = collection.insert_one(doc)
        print(f"Insterted document with ID: {result.inserted_id} successfully")
        return result
    except Exception as e:
        print(f"An error occurred during document insertion: {e}")
        return None
    pass

def create_documents(doc, collection):
    """Insert multiple documents into the collection"""
    try:
        result = collection.insert_many(doc)
        print(f"Insterted documents with IDs: {result.inserted_ids} successfully")
        return result
    except Exception as e:
        print(f"An error occurred during document insertion: {e}")
        return None

#################################### READ #####################################################


def read_all_documents(field: str, value):
    """FETCH AND PRINT ALL DOCUMENTS BY SPECIFIC FIELD."""

    # fileds within 'enrolled_courses'
    enrolled_course_fields = ["course_id", "course_code", "semester", "grade", "status"]
    contact_fields=["email", "phone", "address"] # fields within 'contact_info'
    count_documents=0 # keep count of the documents found

    if field in enrolled_course_fields:
        # Search using the enrolled_courses
        query_field = f"enrolled_courses.{field}"

    elif field in contact_fields:
        # search using contact infomartion
        query_field = f"contact_info.{field}"
    else:
        query_field = field

    query = {query_field: value}
    cursor = collection.find(query) #find document based on query
    documents = list(cursor) # allow us to print out the documents

    if documents:
        print(f"Found {len(documents)} document(s) where '{field}' = '{value}':")
        for doc in documents:
            count_documents+=1
            print(doc)
    else:
        print(f"No documents found where '{field}' = '{value}'.")
    print(f"{count_documents} DOCUMENTS FOUND") # display the amount of documents found
    return documents
    
def findDocumentByID(docID):
    try:
        # Validate the provided id
        objID = ObjectId(docID)
    except Exception as e:
        # provide feedback if ID is invalid
        print(f"Invalid ID format: {e}")
        return None
    
    doc=collection.find_one({"_id": objID}) # find the document in the collection
    if doc:
        # feedback if document is found
        print("Document found")
        print(doc) # print the document
    else:
        # feedback if document not found
        print(" No document found with the provide ID")
    return doc 


########################################## UPDATE #############################################

def update(filter_query: dict[str, any], new_value: dict[str, any]):
    
    try:
        result = collection.update_one(filter_query, {"$set": new_value})
        if result.modified_count > 0:
           print(f" {result.modified_count} documents have been updated successfully.")
        else:
            print("No document matched the filter query.")
    except Exception as e:
        print(f"Error updating document: {e}")
        return None
    

def updateMany(field, new_value, filter_field, filter_value):
    """Update data for many documents that have a specific value for a specific field."""
    
    fields=["course_id", "course_code", "semester", "grade", "status"] # Fields in 'enrolled_courses'
    contact_fields=["email", "phone"] # fields in "contact_info"
    unallowed_fields_to_change=["_id", "student_id"] # FIELDS not allowed to change

    if field in unallowed_fields_to_change:
        print("NOT ALLOWED TO CHANGE THE SPECIFIED FIELD")


    elif field in fields:
    # Filter documents that have a specific value for a field within enrolled_courses
        query = { "enrolled_courses." + filter_field: filter_value }

        cursor = collection.find(query)

        for document in cursor:
            enrolled_courses = document.get("enrolled_courses", [])

            # Loop through the enrolled_courses array and update the field if it matches the filter criteria
            for i, course in enumerate(enrolled_courses):
                if course.get(filter_field) == filter_value:  # Find courses with the specific filter value
                    enrolled_courses[i][field] = new_value  # Update the specified field

            # Update the document in the collection after modifying the enrolled_courses array
            collection.update_one(
                {"_id": document["_id"]},  # Find the document by its _id
                {"$set": {"enrolled_courses": enrolled_courses}}  # Set the modified enrolled_courses
            )

            print(f"Updated enrolled_courses for document {document['_id']} with {filter_field} = {filter_value}:")
        print("All documents updated successfully.")

    elif field in contact_fields:
    # Filter documents that have a specific value for a field within contact
        query = {f"contact_info.{filter_field}": filter_value}

        cursor = collection.find(query)

        for document in cursor:
            contact_info = document.get("contact_info", {})

            # Check if filter_field exists and matches filter_value
            if contact_info.get(filter_field) == filter_value:
                contact_info[field] = new_value  # Update the specified field

                # Update the document in the collection
                collection.update_one(
                    {"_id": document["_id"]},
                    {"$set": {"contact_info": contact_info}}
                )

                print(f"Updated contact_info for document {document['_id']}:")

        # provide feedback
        print("All documents updated successfully.")

    elif field == "name" and filter_field == "student_id":
        # only update name by filtering using 'student_id'

        filter_query = {filter_field: filter_value}
        new_value_dict = {field: new_value}

        result = collection.update_many(filter_query, {"$set": new_value_dict})
        
        # provide feedback
        print(f"{result.modified_count} document(s) updated successfully.")

################################### DELETE ################################################

def delete_document(search_query):
    try:
        result = collection.delete_one(search_query)
        if result.deleted_count > 0:
            print(f"{result.deleted_count} accounts have been deleted")
        else:
            print("No document matched the search query.")
            return result.deleted_count
    except Exception as e:
        print(f"Error deleting document: {e}")
        return 0
    
def delete_documents(studentId):
    """ Delete multiple documents in the collection based on the student_id"""

    result=collection.delete_many({"student_id": studentId}) # deleting many documents

    if result.deleted_count > 0: # check if document is deleted successfully
        # and display message if message successfully deleted
        print(f"{result.deleted_count} accounts have been deleted")
    else:

        # display message if deletion was unsuccessful
        print("No document matched the search query.")



########################################## PRACTICAL 3  ###################################################

def orFunction(collection):
    profs = collection.find({"$or": [{"department": "History"}, {"department": "Biology"}]})
    lst = []
    for prof in profs:
        lst.append(prof)
    return lst


def andFunction(collection, dept):
    courses = collection.find({"$and" : [{"department" : dept} , {"prerequisites" : {"$size" : 0}}]})
    lst = []
    for course in courses:
        lst.append(course)
    return lst

def norFunction(collection):
    students = collection.find({"enrolled_courses": {"$not": {"$elemMatch": {"status": {"$eq": "Completed"}}}}})
    lst = []
    for student in students:
        lst.append(student)
    return lst

def inFunction(collection):
    students = collection.find({"enrolled_courses.semester" : {"$in" : ["Fall 2024", "Spring 2024"]}})
    lst = []
    for student in students:
        lst.append(student)
    return lst

def greaterThan(collection, num):
    students = collection.find({"$expr" : {"$gt" : [{"$size" : "$enrolled courses"}, num]}})
    lst = []
    for student in students:
        lst.append(student)
    return lst

def lessThan(collection, num):
    professors = collection.find({"$expr": {"$lt": [{"$size": {"$ifNull": ["$courses_taught", []]}}, num]}})
    lst = []
    for prof in professors:
        lst.append(prof)
    return lst

def fieldExists(collection):
    courses = collection.find_one({"assigned_professors" : {"$exists" : True}})
    if courses:
        return "Assigned professors field exists"
    else:
        return "Assigned professors field does not exists"


###################### ARRAY FUNCTIONS ###############################################################


def modifyArrayField(studentID, fieldName, valueToAdd):
    """ Add a new element to any array field for a student"""

    results = collection.update_one(
        {"student_id": studentID},
        {"$push": {fieldName: valueToAdd}}
    ) # using $push to insert a new element

    if results.modified_count>0:
        # display message if successfull
        print(f"'{valueToAdd}' added successfully to '{fieldName}' for student {studentID}")
    else:
        # display message if unsuccessfull
        print("Unsuccessfull")

def removeArrayFieldElement(studentID, fieldName, valueToRemove):
    """ Remove an element from any array field for a student """

    results = collection.update_one(
        {"student_id": studentID},
        {"$pull": {fieldName: valueToRemove}}
    )# using $pull to remove the element

    if results.modified_count>0:
        # display message if successfull
        print(f"'{valueToRemove}' removed successfully from '{fieldName}' for student {studentID}")
    else:
        # display message if unsuccessfull
        print("Unsuccessfull")

def findStudentsEnrolledinAllCourses(fieldName, values):
    """ find all students whose array contains all specified values """

    query = {
        fieldName: {
            "$all": values
        }
    }
    result = collection.find(query)
    # display feedback if successfull or unsuccessfull
    found = False # keep track of where students were found or not

    for student in result:
        print(student) # display students if found
        found = True

    if not found:
        print("No matching students found")

def findStudentsWithAllAdvisors(advisorIds):
    """ find students who have all the specified advisor in their advisor array. """
    query = {
        "advisors": {
            "$all": advisorIds
        }
    }

    results = collection.find(query)

    found = False
    for student in results:
        print(student)
        found = True

    if not found:
        print("No matching students found.")

def findStudentsWithArraySize(fieldName, expectedSize):
    """ Find students whose specified array field has exactly 'expected_size' elements.
        field_name can be 'advisors' or 'enrolled_courses'. """
    
    query = {
        fieldName: {
            "$size": expectedSize
        }
    }

    results = collection.find(query)

    found = False
    for student in results:
        print(student)
        found = True

    if not found:
        print("No students found with exactly", expectedSize, fieldName)


####################### Aggregation Pipelines ########################################################


def sumCompletedCourses(collection):
    """ Count the total number of students that completed a course"""
    
    pipeline = collection.aggregate([{"$unwind" : "$enrolled_courses"},
                                     {"$match": {"enrolled_courses.status": "Completed"}},
                                     {"$group" : {"_id" : "$enrolled_courses.semester", "total": {"$sum": 1}}},
                                     {"$project" : { "_id" : 0, "semester": "$_id", "total" : 1}}
                                    ])
    for doc in pipeline:
        # display the documents
        print(doc)

def studentsPerDepartment(collection):
    """Count total number of students per department"""

    pipeline = collection.aggregate([
        {"$match": {"department": {"$exists": True}}},
        {"$group": {"_id": "$department", "total_students": {"$sum": 1}}},
        {"$project": {"_id": 0, "department": "$_id", "total_students": 1}},
        {"$sort": {"total_students": -1}}
    ])

    for doc in pipeline:
        # display the documents
        print(doc)

def sumNotCompletedCourses(collection):
    """ Count the total number of students that haven't completed a course"""
    
    pipeline = collection.aggregate([
        {"$unwind": "$enrolled_courses"},
        {"$match": {"enrolled_courses.status": {"$ne": "Completed"}}},  # not equal to Completed
        {"$group": {"_id": "$enrolled_courses.semester", "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "semester": "$_id", "total": 1}}
        ])

    for doc in pipeline:
        # display the documents
        print(doc)

######################## $LOOKUP #########################################################################

# Pipeline number 1
def studentCoursesWithLookup():
        """ Join students and courses 
            Project a clean view of which students took which course"""
        
        myPipeline=collection.aggregate([
            {"$unwind": "$enrolled_courses"},
            {"$lookup": {
                "from": "courses",
                "localField": "enrolled_courses.course_code",
                "foreignField": "course_code",
                "as": "course_details"
            }},
            {"$unwind": "$course_details"},
            {"$project":{
                "_id": 0,
                "student": "$name",
                "course": "$course_details.course_name",
                "department": "$course_details.department",
                "semester": "$enrolled_courses.semester",
                "grade": "$enrolled_courses.grade"
            }}
        ])
        # display the documents
        for doc in myPipeline:
            print(doc)

# pipeline Number 2
def professorCoursesReport():
    """ Uses $lookup to join professores with courses"""

    myPipeline = collection2.aggregate([
        {"$lookup": {
            "from": "courses",
            "localField": "courses_taught",
            "foreignField": "course_code",
            "as": "taught_courses"
        }},
        {"$project": {
            "_id": 0,
            "professor": "$name",
            "department": 1,
            "taught_courses.course_name": 1,
            "taught_courses.course_code": 1
        }},
        {"$merge": {
            "into": "reports_professor_courses",
            "whenMatched": "merge",
            "whenNotMatched": "insert"
        }}
    ])
    # display the documents
    for doc in myPipeline:
        print(doc)
        

# -------------------------
# Menu System
# -------------------------

def menu():
    while True:
        print("\n--- MongoDB Project Menu ---")
        print("1. Create Document")
        print("2. Read Document")
        print("3. Update Document")
        print("4. Delete Document")
        print("5. Logical Operations") 
        print("6. Aggregation pipelines")
        print("7. Add/Remove a course or Advisor")
        print("8. Exit")

        choice = input("Enter choice: ")

###################################### CREATE #########################################################
        
        if choice == "1":
            numDoc = int(input("Enter number of documents to create: "))
            list_Docs=[] # list all created documents
            if numDoc > 1:
                # creating multiple documents
                for i in range(numDoc+1):
                    if i<1:

                        name=input("Enter student 1st name: ") # prompt for the age
                        age=int(input("Enter student age: ")) # prompt for the age
                    elif i>1: 

                        name=input(f"Enter student {i}nd name: ") # prompt for the name
                        age=int(input("Enter student age: ")) # prompt for the age
                    dict={"name": name, "age": age} # dictionary
                    list_Docs.append(dict) # keep a list of the created documents

                print(list_Docs) # print the created documents
                create_documents(list_Docs, collection)
            else:
                # creating a single document
                name=input("Enter student name: ") # prompt for name
                age=int(input("Enter student age: ")) # prompt for age
                create_document({"name": name, "age": age}, collection)


############################ READ ################################################################
                

        elif choice == "2":
            print("Update:\n 1. One document\n 2. Many Documents")
            myChoice=input("Enter choice: ")

            if myChoice=="2":
                # find all documents matching a simple condition

                field_search_By=input("Enter field name: ")# prompt for the field name
                field_value=input("Enter field value: ") # prompt for the value
                read_all_documents(field_search_By,field_value)

            elif myChoice=="1":
                # Find only one document based on its id

                docId=input("Enter Document _id: ") # prompt user for document _id
                findDocumentByID(docId) # find document based on the provided id


######################## UPDATE #####################################################################
            

        elif choice == "3":

            print("Update:\n 1. One document\n 2. Many Documents")
            myChoice=input("Enter choice: ")

            if myChoice=="1":
                # Updating a single document

                s_name = input("Name of student whose course will be updated: ") # prompt for student name
                course = input("Name of the course: ") # prompt for the name of the course
                update({"name" : s_name}, {"course" : course})

            elif myChoice=="2":
                # Updating multiple documents

                print("PLEASE NOTE: You can only update the 'name' field by 'student_id'.")
                field = input("Name of field to be updated: ") # prompt for the field name
                new_value=input("Enter the new value: ") # prompt for the value
                
                # provide information on how to filter
                filterBy=input("Enter field to filter by: ")
                filterValue=input("Enter Value to filter by: ")
                updateMany(field,new_value,filterBy,filterValue)

########################## DELETE ######################################################################
                
        elif choice == "4":
            print("Delete:\n 1. One document\n 2. Many Documents")

            myChoice=input("Enter choice: ")
            if myChoice=="1": # delete a single document
                
                d_name = input("Enter student name to be deleted: ") 
                # delete based on the name since we are only deleting one document
                delete_document({"name" : d_name})

            elif myChoice=="2": # Delete many documents

                deleteDoc=input("Enter student ID: ") # prompt user to provide student id
                                                      # since we cannot delete documets based on
                                                      # the name since there's a chance that there exist
                                                      # many documents with the same name
                # provide a warining for the deletion
                print("NOTE: you're about to delete all documents associated with the provide student ID")
                userAns=input("Do you wish to continue? ") # prompt the user to terminate or continue
                if userAns[0]=="y" or userAns[0]== "Y":
                    delete_documents(deleteDoc)
                else:
                    continue

######################### LOGICAL OPERATIONS ##################################################


        elif choice == "5":
            print("Which logical operation would you like to perform? \n")
            print("1. AND (Show courses in chosen department that have no prerequisites)")
            print("2. OR (Show History or Biology Professors)")
            print("3. NOT (Show students' courses that are NOT completed)")
            print("4. IN (Show courses that are in the Spring 2024 or Fall 2024 semester)")
            print("5. Greater Than (Show the students who have enrolled courses that is less than a chosen number)")
            print("6. Less Than (Show the professors with less than the chosen number of courses taught)")
            print("7. Exists (Checking if the assigned_professors field exists)")
            print("8. Back")
            
            choice2 = input("Enter choice: ")
            
            if choice2 == "1":
                dept = input("Enter department: ")
                print(andFunction(collection3, dept))
            elif choice2 == "2":
                print(orFunction(collection2))
            elif choice2 == "3":
                print(norFunction(collection))
            elif choice2 == "4":
                print(inFunction(collection))
            elif choice2 == "5":
                num = int(input("Enter number of courses: "))
                print(greaterThan(collection, num))
            elif choice2 == "6":
                num = int(input("Enter number of courses: "))
                print(lessThan(collection2, num))
            elif choice2 == "7":
                print(fieldExists(collection2))
            elif choice == "9":
                menu()
            else:
                print("Invalid choice")
                menu()


##################### ARRAY FUNCTIONS #################################################################


        elif choice=="7":
            print("1. Add new Information.\n" \
            "2. Remove existing information.\n" \
            "3. Find students using $all and $array. ")
            choice4 = input("Enter your choice: ")

            enrrolledCoursesFields=["course_code", "semester", "grade", "status"]

            if choice4 == "1":
                # Add new information (objects or arrays)
                studentID = input("Enter student ID: ")
                fieldName = input(" Select Field to update:\n " \
                            "1. Enrolled Courses.\n" \
                            "2. Advisors.\n"
                            "Enter choice: ")
                
                if fieldName == "1":
                    # if field name is Enrolled_courses
                    new_course = {}

                    for f in enrrolledCoursesFields:
                        # f = field name
                        # prompt the user to enter the info required in "enrolled_courses"

                        value = input(f"Enter {f}: ")
                        new_course[f] = value
                    new_course["course_id"] = ObjectId()
                    modifyArrayField(studentID, "enrolled_courses", new_course)
                
                elif fieldName == "2":

                    advisorID = input("Enter advisor ID (as ObjectID): ")

                    # check if advisorID is valid
                    try:
                        advisorID = ObjectId(advisorID)
                    except:
                        print(" Invalid Advisor ID. Must be a valid ObjectID ")
                        continue
                    modifyArrayField(studentID, "advisors", advisorID)

            elif choice4 == "2":
                # Remove existing information (Objects or arrays))
                studentID = input("Enter student ID: ")
                fieldName = input("Select Field to delete:\n" \
                            "1. Enrolled Courses.\n" \
                            "2. Advisors.\n"
                            "Enter choice: ")
                courseToRemove = {}
                
                if fieldName == "1":
                    courseCode = input("Enter course code to remove: ")
                    courseToRemove = {"course_code": courseCode} # Match objrct for $pull

                    removeArrayFieldElement(studentID, "enrolled_courses", courseToRemove)
                
                elif fieldName == "2":
                    advisorID = input("Enter advisor ID to remove (As ObjectID): ")
                    try:
                        advisorID = ObjectId(advisorID)
                    except:
                        print("Invalid advisor ID. Must be a valid ObjectID")
                        continue
                    removeArrayFieldElement(studentID, "advisors", advisorID)

            elif choice4 == "3":
                
                print("1. Find students with specific advisor IDs.")
                print("2. Find students with exact array size.")
                choice6 = input("Enter choice: ")

                if choice6 == "1":
                    inputIDs = input("Enter advisor ObjectIDs (comma-separated): ")
                    UnfilteredIDs = [i.strip() for i in inputIDs.split(",") if i.strip()]
                    advisorIds = []

                    for id in UnfilteredIDs:
                        try:
                            advisorIds.append(ObjectId(id))
                        except:
                            print(f"{id} Invalid ObjectId. Not Included")

                    if advisorIds:
                        findStudentsWithAllAdvisors(advisorIds)
                    else:
                        print("No valid advisor IDs entered.")

                elif choice6 == "2":
                    field = input("Enter array field to check size (advisors or enrolled_courses): ")
                    try:
                        size = int(input("Enter expected size: "))
                        findStudentsWithArraySize(field, size)
                    except ValueError:
                        print("Invalid Size. Must be a number") 
                else:
                    print("Invalid choice")



##################### AGGREGATION PIPELINES ###########################################################


        elif choice == "6":
            print("---Aggregation pipelines---")

            print("WHICH AGGREGATION PIPELINE WOULD YOU LIKE TO USE?") 

            print("1. Show the sum of completed course per semester.")
            print("2. Show Number of student per department.")
            print("3. Show the sum of students not completed the course per semester.")
            print("4. Show students joined with their courses.")
            print("5. Generate professor-course report and save with $merge.")
            print("6. Back")

            choice3 = input("Enter your choice: ")

            if choice3 == "1":
                print("SHOWING SUM OF COMPLETED COURSES PER SEMESTER")
                sumCompletedCourses(collection)

            elif choice3 == "2":
                print("SHOWING TOTAL NUMBER OF STUDENT PER DEPARTMENT")
                studentsPerDepartment(collection3)
            
            elif choice3 == "3":
                print("SHOWING SUM OF STUDENTS NOT COMPLETED THE COURSE PER SEMESTER")
                sumNotCompletedCourses(collection)
            
            elif choice3 == "4":
                print("SHOWING STUDENTS WITH THEIR COURSES.")
                studentCoursesWithLookup()
            
            elif choice3 == "5":
                print("GENERATING PROFESSOR COURSE REPORT.")
                professorCoursesReport()
                
            elif choice3=="6":
                continue
            else:
                print("INVALID CHOICE")
                

######################## EXIT ############################################################################
        

        elif choice == "8":
            print("Exiting...")
            quit()

        else:
            print("Invalid choice, try again.")


if __name__ == "__main__":
    menu()

# THE CODE IS FORMATED WITH BLACK 