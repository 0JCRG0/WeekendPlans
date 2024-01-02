from utils.handy import *
from utils.bs4_utils import *
from utils.FollowLink import *

def main():
    print("\nHello Maddy (pussylord), do you want to know what we are going to do during the weekend? (yeah/no lame)")
    answer = input()

    if answer.lower() == 'yeah':
        print("\nRight. When do you want to go out? (Frid-yay, Saturday, Sunday or any day if you feel wild)\n")
        day_date = input()
        plan = get_random_plan()
        plan_str = str(plan)
        # Define the characters to be removed
        remove_chars = "()',"
        # Use translate and maketrans to remove the characters
        plan = plan_str.translate(str.maketrans('', '', remove_chars))
        print(f"\nPerfect!! We are going to: {plan} on {day_date}. \n\nPS No doggies allowed soz lames (Cora aka dickhead and fatty aka Lola)")
    else:
        print("\n:( so saaaaad or did you mistype? :))")

if __name__ == "__main__":
    main()